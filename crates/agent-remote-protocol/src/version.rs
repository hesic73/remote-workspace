use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// Stable schema emitted by `agent-remote-server --version-json` and parsed by
/// the client during deployment. `software_version` is the crate release (maps
/// to a CI artifact); `protocol_version` gates client/server compatibility and
/// only changes on incompatible wire or state semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionInfo {
    pub software_version: String,
    pub protocol_version: u32,
}

/// Printed as JSON by `agent-remote-server --install-to` after it runs the
/// locked compare-and-swap on the remote host, and parsed by the client to
/// report what happened. `installed` is true when the new binary was swapped
/// in; false means an equal-or-newer server was already present and kept.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallOutcome {
    pub installed: bool,
    pub previous: Option<VersionInfo>,
    pub current: VersionInfo,
}

/// What a client should do with the server currently installed on a target,
/// given the release it wants to deploy. See DESIGN / onboarding doc section 7.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Preflight {
    /// Installed server is compatible and not older: use it as-is.
    Connect,
    /// No server, a legacy one, or an older one: install the desired release.
    NeedInstall,
    /// Installed protocol is newer than the client can speak: refuse, do not
    /// downgrade. The client must be updated.
    ClientTooOld,
}

/// Client-side decision, evaluated before uploading anything. `installed` is
/// `None` when the target has no managed server (or only a legacy one that does
/// not understand `--version-json`).
pub fn preflight(installed: Option<&VersionInfo>, desired: &VersionInfo) -> Preflight {
    match installed {
        None => Preflight::NeedInstall,
        Some(cur) => match cur.protocol_version.cmp(&desired.protocol_version) {
            Ordering::Greater => Preflight::ClientTooOld,
            Ordering::Less => Preflight::NeedInstall,
            Ordering::Equal => {
                // Same protocol: upgrade only if the installed software is
                // strictly older. A newer or unorderable software version is
                // never downgraded.
                if software_cmp(&cur.software_version, &desired.software_version)
                    == Some(Ordering::Less)
                {
                    Preflight::NeedInstall
                } else {
                    Preflight::Connect
                }
            }
        },
    }
}

/// The monotonic swap rule, evaluated on the remote host under the install lock
/// by the freshly-uploaded binary (whose own version is `desired`). Returns
/// true only when the installed server is strictly older than `desired`; an
/// equal, newer, or unorderable server is never replaced. This is the last
/// guard against a downgrade, independent of any client-side check.
pub fn should_replace(installed: Option<&VersionInfo>, desired: &VersionInfo) -> bool {
    match installed {
        None => true,
        Some(cur) => match cur.protocol_version.cmp(&desired.protocol_version) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => {
                software_cmp(&cur.software_version, &desired.software_version)
                    == Some(Ordering::Less)
            }
        },
    }
}

/// Compare two `MAJOR.MINOR.PATCH` version strings numerically. Returns `None`
/// if either side is not three dot-separated integers -- callers treat an
/// unorderable pair conservatively (never a downgrade) rather than guessing.
/// The release scheme uses plain numeric versions with no pre-release tags.
pub fn software_cmp(a: &str, b: &str) -> Option<Ordering> {
    Some(parse_triple(a)?.cmp(&parse_triple(b)?))
}

fn parse_triple(s: &str) -> Option<(u64, u64, u64)> {
    let mut it = s.split('.');
    let major = it.next()?.parse().ok()?;
    let minor = it.next()?.parse().ok()?;
    let patch = it.next()?.parse().ok()?;
    if it.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(sw: &str, proto: u32) -> VersionInfo {
        VersionInfo {
            software_version: sw.into(),
            protocol_version: proto,
        }
    }

    #[test]
    fn software_cmp_orders_numerically() {
        assert_eq!(software_cmp("0.2.0", "0.10.0"), Some(Ordering::Less));
        assert_eq!(software_cmp("0.2.1", "0.2.1"), Some(Ordering::Equal));
        assert_eq!(software_cmp("1.0.0", "0.9.9"), Some(Ordering::Greater));
        assert_eq!(software_cmp("0.2", "0.2.0"), None);
        assert_eq!(software_cmp("0.2.0", "weird"), None);
    }

    #[test]
    fn preflight_covers_the_table() {
        let desired = v("0.3.0", 3);
        assert_eq!(preflight(None, &desired), Preflight::NeedInstall);
        // older protocol -> install
        assert_eq!(preflight(Some(&v("0.9.0", 2)), &desired), Preflight::NeedInstall);
        // newer protocol -> client too old
        assert_eq!(preflight(Some(&v("0.3.0", 4)), &desired), Preflight::ClientTooOld);
        // equal protocol, older software -> install
        assert_eq!(preflight(Some(&v("0.2.9", 3)), &desired), Preflight::NeedInstall);
        // equal protocol, equal software -> connect
        assert_eq!(preflight(Some(&v("0.3.0", 3)), &desired), Preflight::Connect);
        // equal protocol, newer software -> connect (no downgrade)
        assert_eq!(preflight(Some(&v("0.4.0", 3)), &desired), Preflight::Connect);
    }

    #[test]
    fn should_replace_never_downgrades() {
        let desired = v("0.3.0", 3);
        assert!(should_replace(None, &desired));
        assert!(should_replace(Some(&v("0.2.0", 2)), &desired)); // older proto
        assert!(should_replace(Some(&v("0.2.9", 3)), &desired)); // older sw
        assert!(!should_replace(Some(&v("0.3.0", 3)), &desired)); // equal
        assert!(!should_replace(Some(&v("0.4.0", 3)), &desired)); // newer sw
        assert!(!should_replace(Some(&v("0.1.0", 4)), &desired)); // newer proto
        // unorderable software at equal protocol: do not replace
        assert!(!should_replace(Some(&v("weird", 3)), &desired));
    }
}
