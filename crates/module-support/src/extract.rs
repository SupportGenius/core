//! Turns an uploaded document's bytes into indexable text, by content
//! type — the [`crate::handlers::html_to_text`] path for HTML, UTF-8 for
//! plain text and markdown, and [`lopdf`] for PDFs.
//!
//! This is the `extract` outbox job's whole job: read the parts, join
//! them, and hand text to [`crate::store::insert_source_with_chunks`]. It
//! is deliberately not clever — a PDF it cannot read fails the upload
//! (`failed`), it does not guess. Text here feeds the tokenizer, exactly
//! as for the inline and URL ingest forms.

/// The content types the upload route accepts, as stored on the upload
/// row (lowercased). Anything else is refused at the door with a 415.
pub(crate) const TEXT_PLAIN: &str = "text/plain";
pub(crate) const TEXT_MARKDOWN: &str = "text/markdown";
pub(crate) const TEXT_HTML: &str = "text/html";
pub(crate) const APPLICATION_PDF: &str = "application/pdf";

/// Whether `content_type` is one of the four uploadable types.
pub(crate) fn is_supported(content_type: &str) -> bool {
    matches!(
        content_type,
        TEXT_PLAIN | TEXT_MARKDOWN | TEXT_HTML | APPLICATION_PDF
    )
}

/// The comma-joined list, for error details.
pub(crate) fn supported_list() -> String {
    format!("{TEXT_PLAIN}, {TEXT_MARKDOWN}, {TEXT_HTML}, {APPLICATION_PDF}")
}

/// Reduces `bytes` of `content_type` to text. Every failure is a string
/// for the upload's `error` column: a bad document is the caller's
/// content problem, not an infrastructure fault, and the message should
/// say plainly what could not be read.
pub(crate) fn text_from(content_type: &str, bytes: &[u8]) -> Result<String, String> {
    match content_type {
        TEXT_PLAIN | TEXT_MARKDOWN => utf8(bytes),
        TEXT_HTML => Ok(crate::handlers::html_to_text(utf8(bytes)?.as_str())),
        APPLICATION_PDF => pdf_text(bytes),
        other => Err(format!(
            "unsupported content type {other:?}; expected one of {}",
            supported_list()
        )),
    }
}

/// Strict UTF-8: an upload that is not text is content the tokenizer
/// would mangle into garbage terms, so it fails the upload instead of
/// being lossily re-encoded into noise.
fn utf8(bytes: &[u8]) -> Result<String, String> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| "not valid UTF-8 text".to_owned())
}

/// PDF text via `lopdf` — pure Rust, wasm-safe, so extraction runs in the
/// Worker isolate too. Every page's text operations are collected by
/// lopdf; what survives is the document's text in reading order, which is
/// all the chunker needs. An encrypted, damaged or image-only PDF fails
/// here with lopdf's error text.
fn pdf_text(bytes: &[u8]) -> Result<String, String> {
    let document =
        lopdf::Document::load_mem(bytes).map_err(|err| format!("unreadable PDF: {err}"))?;
    let pages: Vec<u32> = document.get_pages().keys().copied().collect();
    if pages.is_empty() {
        return Err("unreadable PDF: no pages".to_owned());
    }
    document
        .extract_text(&pages)
        .map_err(|err| format!("unreadable PDF: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_and_markdown_decode_strictly() {
        assert_eq!(text_from(TEXT_PLAIN, b"hello").expect("text"), "hello");
        assert_eq!(
            text_from(TEXT_MARKDOWN, b"# heading").expect("markdown"),
            "# heading"
        );
        assert!(text_from(TEXT_PLAIN, b"\xff\xfe").is_err());
    }

    #[test]
    fn html_is_reduced_to_words() {
        let text = text_from(TEXT_HTML, b"<p>a &amp; b</p>").expect("html");
        assert_eq!(text, "a & b");
    }

    #[test]
    fn an_unknown_content_type_is_an_error() {
        assert!(
            text_from("application/octet-stream", b"x")
                .expect_err("unsupported")
                .contains("unsupported content type")
        );
    }

    /// A one-page PDF built through lopdf's own document API and
    /// serialized by its writer, then extracted back to text. The heavy
    /// PDF flows are covered by the integration suite; this pins the
    /// dispatcher to the right branch.
    #[test]
    fn a_pdf_yields_its_text() {
        let pdf = one_page_pdf("lopdf and the support module");
        let text = text_from(APPLICATION_PDF, &pdf).expect("pdf text");
        assert!(text.contains("lopdf and the support module"), "{text:?}");
    }

    #[test]
    fn a_non_pdf_fails_with_a_named_error() {
        let err = text_from(APPLICATION_PDF, b"definitely not a pdf").expect_err("not a pdf");
        assert!(err.contains("unreadable PDF"), "{err}");
    }

    /// A single-page PDF whose page shows `text`. The same shape the
    /// integration suite builds (it cannot see this private module), so
    /// the two must stay in step.
    fn one_page_pdf(text: &str) -> Vec<u8> {
        // The macro must be imported, not path-invoked: lopdf's
        // trailing-comma arm recurses as a bare `dictionary!`, which only
        // resolves when the macro is in scope.
        use lopdf::content::Content;
        use lopdf::dictionary;
        let operations = vec![lopdf::content::Operation::new(
            "Tj",
            vec![lopdf::Object::string_literal(text)],
        )];
        let content = Content { operations };
        let mut doc = lopdf::Document::with_version("1.4");
        let font_id = doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Helvetica",
        });
        let resources_id = doc.add_object(dictionary! {
            "Font" => dictionary! { "F1" => font_id },
        });
        let pages_id = doc.add_object(dictionary! {
            "Type" => "Pages",
            "Kids" => lopdf::Object::Array(Vec::new()),
            "Count" => 1,
        });
        let content_id = doc.add_object(lopdf::Stream::new(
            dictionary! {},
            content.encode().expect("content encodes"),
        ));
        let shown_id = doc.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Contents" => content_id,
            "Resources" => resources_id,
        });
        if let lopdf::Object::Dictionary(kids) = doc.objects.get_mut(&pages_id).expect("pages") {
            kids.set(
                "Kids",
                lopdf::Object::Array(vec![lopdf::Object::Reference(shown_id)]),
            );
        }
        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        doc.trailer.set("Root", catalog_id);
        doc.compress();
        let mut bytes = Vec::new();
        doc.save_to(&mut bytes).expect("serialises");
        bytes
    }
}
