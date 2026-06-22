//! Conversion from xml-oxydizer diagnostic types to protobuf messages.

use xml_oxydizer::diagnostic::{Diagnostic, Severity as OxySeverity};
use xml_oxydizer::tree::path::format_path;

use crate::proto;

/// Converts an xml-oxydizer [`Diagnostic`] into a protobuf [`DiagnosticMessage`](proto::DiagnosticMessage).
pub fn diagnostic_to_proto(d: &Diagnostic, filename: &str) -> proto::DiagnosticMessage {
    proto::DiagnosticMessage {
        rule_name: d.rule_name.clone(),
        message: d.message.clone(),
        element_path: format_path(&d.element_path),
        element_index: d.element_index,
        severity: match d.severity {
            OxySeverity::Error => proto::Severity::Error.into(),
            OxySeverity::Warning => proto::Severity::Warning.into(),
            OxySeverity::Info => proto::Severity::Info.into(),
        },
        filename: filename.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xml_oxydizer::tree::path::PathSegment;

    #[test]
    fn converts_error_diagnostic() {
        let diag = Diagnostic {
            rule_name: "check_attr".to_owned(),
            message: "missing attribute".to_owned(),
            element_path: vec![
                PathSegment::from("root"),
                PathSegment::from("child"),
            ],
            element_index: 3,
            severity: OxySeverity::Error,
        };

        let msg = diagnostic_to_proto(&diag, "test.xml");

        assert_eq!(msg.rule_name, "check_attr");
        assert_eq!(msg.message, "missing attribute");
        assert_eq!(msg.element_path, "root/child");
        assert_eq!(msg.element_index, 3);
        assert_eq!(msg.severity, proto::Severity::Error as i32);
        assert_eq!(msg.filename, "test.xml");
    }

    #[test]
    fn converts_warning_severity() {
        let diag = Diagnostic {
            rule_name: "r".to_owned(),
            message: "m".to_owned(),
            element_path: vec![PathSegment::from("a")],
            element_index: 0,
            severity: OxySeverity::Warning,
        };
        let msg = diagnostic_to_proto(&diag, "f.xml");
        assert_eq!(msg.severity, proto::Severity::Warning as i32);
    }

    #[test]
    fn converts_info_severity() {
        let diag = Diagnostic {
            rule_name: "r".to_owned(),
            message: "m".to_owned(),
            element_path: vec![],
            element_index: 0,
            severity: OxySeverity::Info,
        };
        let msg = diagnostic_to_proto(&diag, "f.xml");
        assert_eq!(msg.severity, proto::Severity::Info as i32);
    }
}
