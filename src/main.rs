// Copyright (c) 2024 Alexey Aristov <aav@acm.org> and others
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at http://
// www.eclipse.org/legal/epl-2.0, or the GNU General Public License, version 3
// which is available at https://www.gnu.org/licenses/gpl-3.0.en.html.
//
// SPDX-License-Identifier: EPL-2.0 OR GPL-3.0

use std::{collections::VecDeque, str::FromStr, time::Duration};

use adaptive_backoff::prelude::{Backoff, BackoffBuilder, ExponentialBackoffBuilder};
use anyhow::Result;
use cli::CLI;
use rumqttc::v5::{
    mqttbytes::{
        v5::{Filter, Packet, Publish},
        QoS,
    },
    AsyncClient, Event, EventLoop, MqttOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    signal::unix::{signal, Signal, SignalKind},
    time::sleep,
};
use tracing::{debug, error, warn};

use knxkit::{
    connection::{connect, ops::GroupOps, KnxBusConnection},
    core::{
        address::{DestinationAddress, GroupAddress},
        apdu::{Service, APDU},
        cemi::CEMI,
        tpdu::TPDU,
        DataPoint,
    },
    project::ProjectExt,
};

use knxkit_dpt::{generic, project::ProjectExtDPT};

mod cli;

struct Mqtt {
    mqtt_client: AsyncClient,
    mqtt_event_loop: EventLoop,
    interrupt: Signal,
}

#[derive(Clone, Debug, Default, Serialize)]
struct MqttGroupMessageOut {
    raw: String,

    dpt: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct MqttGroupMessageIn {
    raw: Option<String>,
    value: Option<Value>,
}

impl Mqtt {
    fn new() -> Self {
        let options = MqttOptions::new("knx2mqtt", &CLI.mqtt_host, cli::CLI.mqtt_port);
        let (mqtt_client, mqtt_event_loop) = AsyncClient::new(options, 16);

        let interrupt = signal(SignalKind::interrupt()).unwrap();

        Self {
            mqtt_client,
            mqtt_event_loop,
            interrupt,
        }
    }

    fn handle_knx(&self, cemi: std::sync::Arc<CEMI>) -> Result<Option<(String, String)>> {
        if let (
            TPDU::DataGroup(APDU {
                service,
                data: Some(data),
                ..
            }),
            DestinationAddress::Group(group),
        ) = (&cemi.npdu.tpdu, &cemi.destination)
        {
            let service = *service;
            let group = *group;

            let project = CLI.project.as_ref();

            if service == Service::GroupValueWrite || service == Service::GroupValueResponse {
                let mut message = MqttGroupMessageOut {
                    raw: data.to_string(),
                    ..Default::default()
                };

                if let Some(dpt) = project.group_dpt(group) {
                    message.dpt = Some(dpt.to_string());
                }

                if let Some(unit) = project.group_dpt_unit(group) {
                    message.unit = Some(unit.to_string());
                }

                if let Some(value) = project.group_json(group, &data) {
                    message.value = Some(value);
                }

                let message = serde_json::to_string(&message).expect("json serialize");

                return Ok(Some((format!("{}/{}", CLI.mqtt_prefix, group), message)));
            }
        }

        Ok(None)
    }

    fn handle_mqtt(&self, publish: Publish) -> Result<Option<(GroupAddress, DataPoint)>> {
        if publish.topic.starts_with(CLI.mqtt_prefix.as_bytes()) {
            let project = CLI.project.as_ref();

            let group = GroupAddress::from_str(&String::from_utf8_lossy(
                &publish.topic[CLI.mqtt_prefix.len() + 1..],
            ))?;

            let message = serde_json::from_slice::<MqttGroupMessageIn>(&publish.payload)?;
            let dpt = project.group_dpt(group);

            if dpt.is_none() && CLI.ignore_unknown {
                return Ok(None);
            }

            match (message.raw, message.value, dpt) {
                (None, Some(value), Some(dpt)) => {
                    let data_point = generic::try_decode_json(dpt, value)?.to_data_point();

                    Ok(Some((group, data_point)))
                }

                (Some(raw), None, _) => {
                    let data_point = DataPoint::from_str(&raw)?;
                    Ok(Some((group, data_point)))
                }

                (Some(raw), Some(value), None) => {
                    warn!(
                        raw,
                        ?value,
                        "both raw and value fields are set, but dpt is known. ignoring"
                    );
                    Ok(None)
                }

                (Some(raw), Some(value), Some(dpt)) => {
                    let data_point1 = generic::try_decode_json(dpt, value.clone())?.to_data_point();
                    let data_point2 = DataPoint::from_str(&raw)?;

                    if data_point1 == data_point2 {
                        Ok(Some((group, data_point1)))
                    } else {
                        warn!(
                            raw,
                            ?value,
                            "both raw and value fields are set, but dont match. ignoring"
                        );
                        Ok(None)
                    }
                }

                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// returns true if outer loop should continue
    async fn run_loop(&mut self, connection: &mut impl KnxBusConnection) -> bool {
        let mut filter = Filter::new(format!("{}/#", CLI.mqtt_prefix), QoS::AtLeastOnce);
        filter.nolocal = true;

        self.mqtt_client.subscribe_many([filter]).await.unwrap();

        let mut request_queue = VecDeque::new();

        if CLI.initial_request {
            if let Some(project) = CLI.project.as_ref() {
                for group in project.groups.groups.iter() {
                    request_queue.push_back(group.address);
                }

                debug!("initial request for {} groups", request_queue.len());
            }
        }

        loop {
            tokio::select! {
                _ = self.interrupt.recv() => {
                    _ = self.mqtt_client.disconnect().await;
                    debug!("interrupt signal, terminating");
                    break false;
                }

                poll = self.mqtt_event_loop.poll() => {
                    match poll {
                        Ok(Event::Incoming(Packet::Publish(publish))) if !publish.retain=> {
                            match self.handle_mqtt(publish) {
                                Ok(Some((group_address, data_point))) => {
                                    debug!(group=?group_address, ?data_point, "knx group write");

                                    if let Err(error) = connection.group_write(group_address, data_point).await {
                                        error!(%error, "cannot write to group")
                                    }
                                }

                                Ok(None) => {
                                    // nothing to forward
                                }

                                Err(error) => {
                                    warn!(%error, "cannot handle incoming mqtt message");
                                }
                            }
                        }

                        Ok(_) => {
                            // ignore
                        }

                        Err(error) => {
                            warn!(%error, "mqtt error");
                        }
                    };
                }

                _ = tokio::time::sleep(if request_queue.is_empty() {
                    Duration::MAX
                } else {
                    CLI.initial_request_delay
                }) => {
                    if let Some(group) = request_queue.pop_front() {
                        let _ = connection.group_request(group).await;
                    }
                }

                recv = connection.recv() => {
                    if let Some(cemi) = recv {
                        match self.handle_knx(cemi) {
                            Ok(Some((topic, message))) => {
                                debug!(topic, message, "mqtt message");

                                self.mqtt_client.publish(topic, QoS::AtLeastOnce, true, message,).await.unwrap();
                            }

                            Ok(None) => {
                                // nothing to forward
                            }

                            Err(error) => {
                                warn!(%error, "cannot handle incoming knx message");
                            }
                        }
                    } else {
                        debug!("bus connection closed");
                        break true;
                    }
                }
            }
        }
    }

    async fn run(&mut self) -> Result<()> {
        let mut backoff = ExponentialBackoffBuilder::default()
            .min(Duration::from_secs(1))
            .max(Duration::from_secs(60))
            .build()?;

        loop {
            if let Ok(mut connection) = connect(CLI.local_address, &CLI.remote).await {
                backoff.reset();

                let result = self.run_loop(&mut connection).await;
                connection.terminate().await;

                if !result {
                    break Ok(());
                }
            } else {
                sleep(backoff.wait()).await;
                continue;
            };
        }
    }
}

#[tokio::main]
async fn main() {
    if CLI.log {
        use tracing_subscriber::filter::targets::Targets;
        use tracing_subscriber::prelude::*;

        let filter = Targets::default().with_target("knx2mqtt", tracing::Level::DEBUG);

        let format = tracing_subscriber::fmt::layer().compact();

        tracing_subscriber::registry()
            .with(filter)
            .with(format)
            .init();
    }

    Mqtt::new().run().await.unwrap();
}
