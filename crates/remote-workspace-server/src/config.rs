use std::collections::HashMap;

use remote_workspace_protocol::{ErrorCode, ProtocolError};
use serde::Deserialize;

/// Strict parsing throughout: an unknown field means the config was written
/// for a different (likely newer) server, and silently ignoring it would run
/// commands in the wrong environment.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
    /// Profile applied when a request names none. Must reference a declared
    /// profile; validated at load, not first use.
    #[serde(default)]
    pub default_profile: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// Command vector the command runs through; the server appends the
    /// generated `setup + exec argv` script as its final argument. Lets a
    /// profile load the user's real shell environment, e.g. ["zsh", "-lic"].
    #[serde(default = "default_shell")]
    pub shell: Vec<String>,
    pub setup: String,
}

#[cfg(unix)]
fn default_shell() -> Vec<String> {
    vec!["bash".into(), "-c".into()]
}

#[cfg(windows)]
fn default_shell() -> Vec<String> {
    vec![
        "powershell.exe".into(),
        "-NoLogo".into(),
        "-NoProfile".into(),
        "-NonInteractive".into(),
        "-Command".into(),
    ]
}

impl ServerConfig {
    pub fn load_from_str(s: &str) -> Result<Self, ProtocolError> {
        if s.trim().is_empty() {
            return Ok(Self::default());
        }
        let config: ServerConfig = toml::from_str(s).map_err(|e| {
            ProtocolError::new(
                ErrorCode::InvalidRequest,
                format!("invalid server config: {e}"),
            )
        })?;
        for (name, profile) in &config.profiles {
            if profile.shell.is_empty() {
                return Err(ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    format!("invalid server config: profile '{name}' has an empty shell"),
                ));
            }
        }
        if let Some(name) = &config.default_profile {
            if !config.profiles.contains_key(name) {
                return Err(ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    format!(
                        "invalid server config: default_profile '{name}' is not a declared profile"
                    ),
                ));
            }
        }
        Ok(config)
    }

    /// The profile a request runs under: the explicitly requested one, else
    /// the configured default, else none (direct spawn of the argv).
    pub fn profile_for(&self, requested: Option<&str>) -> Result<Option<&Profile>, ProtocolError> {
        match requested.or(self.default_profile.as_deref()) {
            None => Ok(None),
            Some(name) => self.profiles.get(name).map(Some).ok_or_else(|| {
                ProtocolError::new(
                    ErrorCode::InvalidRequest,
                    format!("unknown profile: {name}"),
                )
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn shell_defaults_to_bash() {
        let c = ServerConfig::load_from_str("[profiles.p]\nsetup = \"x=1\"\n").unwrap();
        assert_eq!(c.profiles["p"].shell, vec!["bash", "-c"]);
    }

    #[test]
    #[cfg(windows)]
    fn shell_defaults_to_powershell() {
        let c = ServerConfig::load_from_str("[profiles.p]\nsetup = \"$x = 1\"\n").unwrap();
        assert_eq!(
            c.profiles["p"].shell,
            vec![
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command"
            ]
        );
    }

    #[test]
    fn unknown_fields_rejected_everywhere() {
        for bad in [
            "surprise = 1\n",
            "[profiles.p]\nsetup = \"\"\nenv = { A = \"1\" }\n",
        ] {
            let err = ServerConfig::load_from_str(bad).unwrap_err();
            assert_eq!(err.code, ErrorCode::InvalidRequest, "accepted: {bad}");
        }
    }

    #[test]
    fn empty_shell_rejected() {
        let err =
            ServerConfig::load_from_str("[profiles.p]\nshell = []\nsetup = \"\"\n").unwrap_err();
        assert!(err.message.contains("empty shell"), "{err}");
    }

    #[test]
    fn undeclared_default_profile_rejected_at_load() {
        let err = ServerConfig::load_from_str("default_profile = \"ghost\"\n").unwrap_err();
        assert!(err.message.contains("ghost"), "{err}");
    }

    #[test]
    fn default_profile_applies_only_when_none_requested() {
        let c = ServerConfig::load_from_str(
            "default_profile = \"a\"\n\
             [profiles.a]\nsetup = \"A=1\"\n\
             [profiles.b]\nsetup = \"B=1\"\n",
        )
        .unwrap();
        assert_eq!(c.profile_for(None).unwrap().unwrap().setup, "A=1");
        assert_eq!(c.profile_for(Some("b")).unwrap().unwrap().setup, "B=1");
        assert!(c.profile_for(Some("nope")).is_err());
    }

    #[test]
    fn no_profile_and_no_default_is_direct() {
        let c = ServerConfig::load_from_str("[profiles.p]\nsetup = \"\"\n").unwrap();
        assert!(c.profile_for(None).unwrap().is_none());
    }
}
