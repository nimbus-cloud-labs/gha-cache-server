use std::borrow::Cow;

use crate::config::DatabaseDriver;

use core::ops::ControlFlow;
use sqlparser::ast::{Expr, Value, VisitMut, VisitorMut};
use sqlparser::dialect::{Dialect, GenericDialect, MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
use tracing::debug;

pub fn rewrite_placeholders<'q>(sql: &'q str, driver: DatabaseDriver) -> Cow<'q, str> {
    if driver != DatabaseDriver::Postgres || !sql.contains('?') {
        return Cow::Borrowed(sql);
    }

    let postgres = PostgreSqlDialect {};
    let mysql = MySqlDialect {};
    let generic = GenericDialect {};
    let dialects: [&dyn Dialect; 3] = [&postgres, &mysql, &generic];
    let mut last_error = None;

    for dialect in dialects {
        match Parser::parse_sql(dialect, sql) {
            Ok(mut statements) => {
                let mut rewriter = PlaceholderRewriter::new();
                let _ = VisitMut::visit(&mut statements, &mut rewriter);

                if !rewriter.replaced {
                    return Cow::Borrowed(sql);
                }

                let rewritten = statements
                    .into_iter()
                    .map(|statement| statement.to_string())
                    .collect::<Vec<_>>()
                    .join("; ");

                let trimmed_start = sql.trim_start_matches(char::is_whitespace);
                let leading_len = sql.len() - trimmed_start.len();
                let leading_ws = &sql[..leading_len];
                let trimmed_end = trimmed_start.trim_end_matches(char::is_whitespace);
                let trailing_ws = &trimmed_start[trimmed_end.len()..];
                let had_semicolon = trimmed_end.ends_with(';');

                let mut output = String::with_capacity(sql.len() + 8);
                output.push_str(leading_ws);
                output.push_str(&rewritten);

                if had_semicolon {
                    output.push(';');
                }

                output.push_str(trailing_ws);

                return Cow::Owned(output);
            }
            Err(err) => last_error = Some(err),
        }
    }

    if let Some(err) = last_error {
        debug!(error = %err, "failed to parse SQL for placeholder rewrite");
    }

    Cow::Borrowed(sql)
}

struct PlaceholderRewriter {
    next_index: usize,
    replaced: bool,
}

impl PlaceholderRewriter {
    fn new() -> Self {
        Self {
            next_index: 1,
            replaced: false,
        }
    }
}

impl VisitorMut for PlaceholderRewriter {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(value) = expr
            && let Value::Placeholder(placeholder) = &mut value.value
            && placeholder.starts_with('?')
        {
            *placeholder = format!("${}", self.next_index);
            self.next_index += 1;
            self.replaced = true;
        }

        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaves_non_postgres_queries_untouched() {
        let sql = "SELECT ?";
        let rewritten = rewrite_placeholders(sql, DatabaseDriver::Mysql);
        assert!(matches!(
            rewritten,
            Cow::Borrowed(returned) if std::ptr::eq(returned, sql)
        ));
    }

    #[test]
    fn rewrites_placeholders_for_postgres() {
        let sql = "SELECT ? FROM cache_entries WHERE id = ?";
        let rewritten = rewrite_placeholders(sql, DatabaseDriver::Postgres);
        assert_eq!(
            rewritten.as_ref(),
            "SELECT $1 FROM cache_entries WHERE id = $2"
        );
    }

    #[test]
    fn ignores_question_marks_inside_literals() {
        let sql = "SELECT '?' AS literal, ? AS param";
        let rewritten = rewrite_placeholders(sql, DatabaseDriver::Postgres);
        assert_eq!(rewritten.as_ref(), "SELECT '?' AS literal, $1 AS param");
    }

    #[test]
    fn falls_back_to_original_when_parse_fails() {
        let sql = "SELECT ? FROM";
        let rewritten = rewrite_placeholders(sql, DatabaseDriver::Postgres);
        assert!(matches!(
            rewritten,
            Cow::Borrowed(returned) if std::ptr::eq(returned, sql)
        ));
    }
}
