// Copyright © 2022 The Radicle Link Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod data;
pub use data::Data;

pub mod track;
pub use track::Track;

pub enum Event {
    Track,
    Data,
}

pub trait IsZero {
    fn is_zero(&self) -> bool;
}

// TODO(finto): Hook execution with arguments - end of process 0x04
// TODO(finto): Test the roundtrip of Data and Track
// TODO(finto): Test the API surfact of Data and Track
