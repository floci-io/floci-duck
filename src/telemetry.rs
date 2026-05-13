use std::fmt;
use tracing::field::{Field, Visit};
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::FormatFields;

/// Formats span/event fields with `correlation_id` shown as a bare value.
/// `query{correlation_id=abc}` becomes `query{abc}`.
/// All other fields keep the standard `key=value` form.
pub struct CorrelationFields;

struct FieldVisitor<'a> {
    writer: Writer<'a>,
    is_empty: bool,
    result: fmt::Result,
}

impl<'a> FieldVisitor<'a> {
    fn new(writer: Writer<'a>) -> Self {
        Self { writer, is_empty: true, result: Ok(()) }
    }

    fn write_field(&mut self, field: &Field, args: fmt::Arguments<'_>) {
        if self.result.is_err() {
            return;
        }
        match field.name() {
            "correlation_id" => {
                self.result = write!(self.writer, "{}", args);
            }
            "message" => {
                if !self.is_empty {
                    self.result = write!(self.writer, " ");
                    if self.result.is_err() { return; }
                }
                self.result = write!(self.writer, "{}", args);
            }
            _ => {
                if !self.is_empty {
                    self.result = write!(self.writer, " ");
                    if self.result.is_err() { return; }
                }
                self.result = write!(self.writer, "{}={}", field.name(), args);
            }
        }
        self.is_empty = false;
    }
}

impl Visit for FieldVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.write_field(field, format_args!("{}", value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.write_field(field, format_args!("{}", value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.write_field(field, format_args!("{}", value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.write_field(field, format_args!("{}", value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.write_field(field, format_args!("{}", value));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.write_field(field, format_args!("{:?}", value));
    }
}

impl<'writer> FormatFields<'writer> for CorrelationFields {
    fn format_fields<R: RecordFields>(
        &self,
        writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        let mut visitor = FieldVisitor::new(writer);
        fields.record(&mut visitor);
        visitor.result
    }
}
