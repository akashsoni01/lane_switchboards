//! Generated protobuf types and conversions to crate mesh types.

pub mod control {
    tonic::include_proto!("lane_switchboard.control");
}
pub mod data {
    tonic::include_proto!("lane_switchboard.data");
}
pub mod paxos {
    tonic::include_proto!("lane_switchboard.paxos");
}
pub mod storage {
    tonic::include_proto!("lane_switchboard.storage");
}

use crate::mesh::ServiceRecord;

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn require_field(value: &str, field: &str) -> Result<(), String> {
    if value.is_empty() {
        Err(format!("missing required field: {field}"))
    } else {
        Ok(())
    }
}

impl TryFrom<control::ServiceRecord> for ServiceRecord {
    type Error = String;

    fn try_from(p: control::ServiceRecord) -> Result<Self, Self::Error> {
        require_field(&p.service, "service")?;
        require_field(&p.instance_id, "instance_id")?;
        require_field(&p.address, "address")?;

        let target = if p.target.is_empty() {
            p.instance_id.clone()
        } else {
            p.target
        };

        let dc = if p.dc.is_empty() {
            None
        } else {
            Some(p.dc)
        };

        Ok(ServiceRecord {
            service: p.service,
            instance_id: p.instance_id,
            address: p.address,
            target,
            dc,
            registered_at: unix_now(),
        })
    }
}

impl From<ServiceRecord> for control::ServiceRecord {
    fn from(r: ServiceRecord) -> Self {
        Self {
            service: r.service,
            instance_id: r.instance_id,
            address: r.address,
            target: r.target,
            dc: r.dc.unwrap_or_default(),
        }
    }
}

impl TryFrom<control::RegisterRequest> for ServiceRecord {
    type Error = String;

    fn try_from(p: control::RegisterRequest) -> Result<Self, Self::Error> {
        ServiceRecord::try_from(control::ServiceRecord {
            service: p.service,
            instance_id: p.instance_id,
            address: p.address,
            target: p.target,
            dc: p.dc,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record() -> ServiceRecord {
        ServiceRecord {
            service: "orders".into(),
            instance_id: "orders-1".into(),
            address: "127.0.0.1:9000".into(),
            target: "orders-1".into(),
            dc: Some("east".into()),
            registered_at: 1_700_000_000,
        }
    }

    #[test]
    fn service_record_round_trip() {
        let original = sample_record();
        let proto: control::ServiceRecord = original.clone().into();
        let back = ServiceRecord::try_from(proto).expect("try_from");

        assert_eq!(back.service, original.service);
        assert_eq!(back.instance_id, original.instance_id);
        assert_eq!(back.address, original.address);
        assert_eq!(back.target, original.target);
        assert_eq!(back.dc, original.dc);
    }

    #[test]
    fn missing_service_field_errors() {
        let proto = control::ServiceRecord {
            service: String::new(),
            instance_id: "id".into(),
            address: "127.0.0.1:1".into(),
            target: "id".into(),
            dc: String::new(),
        };
        let err = ServiceRecord::try_from(proto).unwrap_err();
        assert!(err.contains("service"));
    }

    #[test]
    fn dc_field_round_trips() {
        let with_dc = sample_record();
        let proto: control::ServiceRecord = with_dc.clone().into();
        assert_eq!(proto.dc, "east");
        let back = ServiceRecord::try_from(proto).expect("with dc");
        assert_eq!(back.dc.as_deref(), Some("east"));

        let mut no_dc = with_dc;
        no_dc.dc = None;
        let proto: control::ServiceRecord = no_dc.into();
        assert!(proto.dc.is_empty());
        let back = ServiceRecord::try_from(proto).expect("no dc");
        assert!(back.dc.is_none());
    }
}
