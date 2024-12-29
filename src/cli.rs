// Copyright (c) 2024 Alexey Aristov <aav@acm.org> and others
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at http://
// www.eclipse.org/legal/epl-2.0, or the GNU General Public License, version 3
// which is available at https://www.gnu.org/licenses/gpl-3.0.en.html.
//
// SPDX-License-Identifier: EPL-2.0 OR GPL-3.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::LazyLock,
};

use anyhow::{bail, Result};
use clap::Parser;

use knxkit::{connection::remote, project::Project};

fn parse_local(v: &str) -> Result<Ipv4Addr> {
    let local = if v != "auto" {
        v.parse::<Ipv4Addr>()?
    } else {
        if let Ok(IpAddr::V4(v4)) = local_ip_address::local_ip() {
            v4
        } else {
            bail!("cannot identify local ip v4 address")
        }
    };

    Ok(local)
}

fn parse_project(v: &str) -> Result<Project> {
    Ok(Project::open(v)?)
}

#[derive(Parser, Debug, Clone)]
pub struct Cli {
    #[arg(short = 'l', long = "local")]
    #[arg(value_parser = parse_local, default_value="auto")]
    pub local_address: Ipv4Addr,

    #[arg(long = "log")]
    pub log: bool,

    #[arg(long)]
    #[arg(value_parser = parse_project)]
    pub project: Option<Project>,

    #[arg(long)]
    #[arg(value_parser = remote::parse_remote)]
    pub remote: remote::RemoteSpec,

    #[arg(long)]
    pub mqtt_host: String,

    #[arg(long, default_value = "1883")]
    pub mqtt_port: u16,

    #[arg(long, default_value = "knx/group")]
    pub mqtt_prefix: String,

    #[arg(long)]
    pub ignore_unknown: bool,
}

pub static CLI: LazyLock<Cli> = LazyLock::new(Cli::parse);
