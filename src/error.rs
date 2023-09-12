use std::io;
use std::path::PathBuf;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SunsetDBError {
    #[error("there should be at least a segment")]
    NoSegments,

    #[error("segment error")]
    SegmentError(#[from] SegmentError),

    #[error("IO error")]
    IOError(#[from] io::Error),
}

#[derive(Error, Debug)]
pub enum InsertError {
    #[error("there should be at least a segment")]
    NoSegments,

    #[error("key exceeds max size (expected < {})", u64::MAX)]
    KeyExceedsMaxSize,

    #[error("value exceeds max size (expected < {})", u64::MAX)]
    ValueExceedsMaxSize,

    #[error("IO error")]
    IOError(#[from] io::Error),
}

#[derive(Error, Debug)]
pub enum DeleteError {
    #[error("there should be at least a segment")]
    NoSegments,

    #[error("key not found")]
    KeyNotFound,

    #[error("IO error")]
    IOError(#[from] io::Error),
}

#[derive(Error, Debug)]
pub enum GetError {
    #[error("key not found")]
    KeyNotFound,

    #[error("invalid checksum (expected {expected:?}, found {found:?})")]
    InvalidChecksum { expected: u32, found: u32 },

    #[error("read error")]
    ReadError(#[from] ReadError),
}

#[derive(Error, Debug)]
pub enum SegmentError {
    #[error("can't create segment from path")]
    InvalidPath(PathBuf),

    #[error("invalid index format: {0:?}")]
    InvalidIndexFormat(String),

    #[error("seek error")]
    SeekError,

    #[error("read error")]
    ReadError(#[from] ReadError),

    #[error("IO error at path: {path}")]
    IOErrorAtPath { path: PathBuf, source: io::Error },

    #[error("IO error")]
    IOError(#[from] io::Error),
}

#[derive(Error, Debug, PartialEq)]
pub(crate) enum SegmentIDError {
    #[error("ID is not an int")]
    NotAnInt,

    #[error("trying to parse ID from an empty path")]
    IDFromEmtpyPath,

    #[error("trying to parse ID from an invalid (non-utf8) path: {0}")]
    IDFromInvalidPath(PathBuf),
}

#[derive(Error, Debug)]
pub enum ReadError {
    #[error("invalid checksum (expected {expected:?}, found {found:?})")]
    InvalidChecksum { expected: u32, found: u32 },

    #[error("IO error")]
    IOError(#[from] io::Error),

    #[error("invalid string")]
    InvalidString {
        #[from]
        source: std::string::FromUtf8Error,
    },

    #[error("invalid int")]
    InvalidInt(#[from] std::num::TryFromIntError),
}
