use std::fmt;

use anyhow::{Result, bail};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// Bytes of tag at the end of every message.
pub const TAG_LEN: usize = 32;

/// Shortest secret taken. Anyone who captures one message can guess keys
/// against its tag offline, as fast as their hardware goes, so a short or
/// guessable key doesn't stay secret for long.
pub const MIN_SECRET_LEN: usize = 32;

/// The secret every member of a cluster shares.
///
/// Every message, UDP or TCP, ends with a tag computed from the key and the
/// message's own bytes. Making a tag that matches needs the key, so a node
/// drops anything whose tag is wrong: whoever sent it isn't one of us, or
/// changed the message on the way. Without this, anyone who can reach the
/// port can join, add members that don't exist, or declare real ones dead.
///
/// Signs, doesn't encrypt. Someone watching the network can still read the
/// messages, but all that's in them is ids, addresses and states.
#[derive(Clone)]
pub struct ClusterKey(Hmac<Sha256>);

impl ClusterKey {
    /// The key from what `--key` was given. The error never repeats the
    /// secret, since errors end up on screens and in logs.
    pub fn from_secret(secret: &str) -> Result<Self> {
        if secret.len() < MIN_SECRET_LEN {
            bail!(
                "--key is {} characters, needs at least {MIN_SECRET_LEN}. \
                 `openssl rand -hex 32` makes a good one",
                secret.len()
            );
        }
        Ok(Self::new(secret.as_bytes()))
    }

    fn new(secret: &[u8]) -> Self {
        // HMAC takes a key of any length, so this can't fail
        Self(Hmac::new_from_slice(secret).expect("HMAC accepts any key length"))
    }

    /// For a cluster run without `--key`. Messages are still tagged, the same
    /// way, but with an empty key that anyone can use, so it proves nothing.
    ///
    /// Tagging anyway keeps one message layout. A node with a key and a node
    /// without then reject each other loudly, instead of one of them quietly
    /// accepting everything.
    pub fn none() -> Self {
        Self::new(&[])
    }

    /// `message` with its tag on the end.
    pub fn seal(&self, mut message: Vec<u8>) -> Vec<u8> {
        let mut mac = self.0.clone();
        mac.update(&message);
        message.extend_from_slice(&mac.finalize().into_bytes());
        message
    }

    /// The message inside `sealed`, if its tag is right.
    pub fn open<'a>(&self, sealed: &'a [u8]) -> Result<&'a [u8]> {
        let Some(split) = sealed.len().checked_sub(TAG_LEN) else {
            bail!("message is too short to carry a tag");
        };
        let (message, tag) = sealed.split_at(split);

        let mut mac = self.0.clone();
        mac.update(message);
        // compares in constant time. an `==` stops at the first wrong byte, and
        // timing that is how a tag gets guessed one byte at a time
        if mac.verify_slice(tag).is_err() {
            bail!("message failed authentication: is --key the same on every node?");
        }
        Ok(message)
    }
}

/// Never prints the key: `LocalNode` holds one, and gets logged with `{:?}`.
impl fmt::Debug for ClusterKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClusterKey(..)")
    }
}
