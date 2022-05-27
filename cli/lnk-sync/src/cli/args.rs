// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use librad::git::Urn;

use crate::Mode;

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Args {
    Sync {
        #[clap(long)]
        urn: Urn,
        #[clap(long, default_value_t)]
        mode: Mode,
    },
    Clone {
        #[clap(long)]
        urn: Urn,
        #[clap(long)]
        path: Option<std::path::PathBuf>,
        #[clap(long)]
        peer: Option<librad::PeerId>,
    },
}
