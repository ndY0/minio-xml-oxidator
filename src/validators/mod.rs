//! Concrete validator definitions built with the xml-oxydizer `TreeBuilder` API.
//!
//! Each validator is a function returning a `DescriptorTree`. All known
//! validators are wired into the registry via [`register_all`].

use xml_oxydizer::diagnostic::{Diagnostic, Severity};
use xml_oxydizer::rule::{NodeAccess, Rule};
use xml_oxydizer::tree::builder::TreeBuilder;
use xml_oxydizer::tree::descriptor::NodeNeeds;

use crate::registry::ValidatorRegistry;

/// Registers all built-in validators into the given registry.
pub fn register_all(registry: &mut ValidatorRegistry) {
    registry.register("example-catalog", build_catalog_validator());
}

/// Fails if the element is missing a required attribute.
struct RequireAttr {
    attr_name: &'static str,
}

impl Rule for RequireAttr {
    fn name(&self) -> &str {
        "require_attr"
    }
    fn needs(&self) -> NodeNeeds {
        NodeNeeds::ATTRS
    }
    fn evaluate(&self, node: &dyn NodeAccess) -> Vec<Diagnostic> {
        if node.attr(self.attr_name).is_some() {
            vec![]
        } else {
            vec![Diagnostic {
                rule_name: self.name().to_owned(),
                severity: Severity::Error,
                message: format!("missing required attribute '{}'", self.attr_name),
                element_path: node.path().to_vec(),
                element_index: node.element_index() as u32,
            }]
        }
    }
}

/// Warns if the element has no child elements.
struct RequireChildren;

impl Rule for RequireChildren {
    fn name(&self) -> &str {
        "require_children"
    }
    fn needs(&self) -> NodeNeeds {
        NodeNeeds::CHILDREN
    }
    fn evaluate(&self, node: &dyn NodeAccess) -> Vec<Diagnostic> {
        if node.children_summaries().is_empty() {
            vec![Diagnostic {
                rule_name: self.name().to_owned(),
                severity: Severity::Warning,
                message: "element has no children".to_owned(),
                element_path: node.path().to_vec(),
                element_index: node.element_index() as u32,
            }]
        } else {
            vec![]
        }
    }
}

/// Builds the "example-catalog" validator tree.
///
/// Expects XML shaped like:
/// ```xml
/// <catalog version="...">
///   <entry id="..."/>
/// </catalog>
/// ```
fn build_catalog_validator() -> xml_oxydizer::tree::descriptor::DescriptorTree<Box<dyn Rule>> {
    TreeBuilder::new("catalog")
        .streaming()
        .rule(Box::new(RequireAttr {
            attr_name: "version",
        }) as Box<dyn Rule>)
        .rule(Box::new(RequireChildren) as Box<dyn Rule>)
        .node("entry")
        .streaming()
        .rule(Box::new(RequireAttr { attr_name: "id" }) as Box<dyn Rule>)
        .done()
        .build()
        .expect("example-catalog validator definition is invalid")
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use crossbeam_channel::bounded;
    use xml_oxydizer::diagnostic::Diagnostic;
    use xml_oxydizer::pipeline::{FileInfo, PipelineConfig, run_pipeline};

    use super::*;

    fn validate_xml(xml: &str) -> Vec<Diagnostic> {
        let tree = Arc::new(build_catalog_validator());
        let xml_bytes = xml.as_bytes().to_vec();
        let (tx, rx) = bounded(1024);
        let errors = run_pipeline(
            vec![FileInfo {
                filename: "test.xml".to_owned(),
                descriptors: tree,
                stream_factory: Box::new(move || {
                    Box::new(Cursor::new(xml_bytes)) as Box<dyn std::io::Read + Send>
                }),
            }],
            tx,
            &PipelineConfig::default(),
        );
        assert!(errors.is_empty(), "pipeline errors: {:?}", errors);
        rx.try_iter().collect()
    }

    #[test]
    fn valid_catalog_produces_no_diagnostics() {
        let diags = validate_xml(r#"<catalog version="1"><entry id="a"/></catalog>"#);
        assert!(diags.is_empty(), "unexpected: {:?}", diags);
    }

    #[test]
    fn missing_version_attr_reports_error() {
        let diags = validate_xml(r#"<catalog><entry id="a"/></catalog>"#);
        assert!(diags.iter().any(|d| d.rule_name == "require_attr"
            && d.message.contains("version")));
    }

    #[test]
    fn missing_entry_id_reports_error() {
        let diags = validate_xml(r#"<catalog version="1"><entry/></catalog>"#);
        assert!(diags.iter().any(|d| d.rule_name == "require_attr"
            && d.message.contains("id")));
    }

    #[test]
    fn empty_catalog_warns_no_children() {
        let diags = validate_xml(r#"<catalog version="1"/>"#);
        assert!(diags.iter().any(|d| d.rule_name == "require_children"));
    }

    #[test]
    fn register_all_populates_registry() {
        let mut reg = crate::registry::ValidatorRegistry::new();
        register_all(&mut reg);
        assert!(reg.get("example-catalog").is_some());
    }
}
