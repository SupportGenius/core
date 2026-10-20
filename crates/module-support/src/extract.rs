//! Turns an uploaded document's bytes into indexable text, by content
//! type — the [`crate::handlers::html_to_text`] path for HTML, UTF-8 for
//! plain text and markdown, and [`lopdf`] for PDFs.
//!
//! This is the `extract` outbox job's whole job: read the parts, join
//! them, and hand text to [`crate::store::insert_source_with_chunks`]. It
//! is deliberately not clever — a PDF it cannot read fails the upload
//! (`failed`), it does not guess. Text here feeds the tokenizer, exactly
//! as for the inline and URL ingest forms.
//!
//! The declared content type is a claim, not a fact. Every upload is
//! sniffed against its bytes: a PDF must carry a `%PDF-` header, and a
//! text type must be UTF-8 with no NUL byte and no PDF signature. The
//! first part is checked as it arrives ([`first_part_mismatch`]); the
//! whole document is checked again at extraction ([`text_from`]), since
//! a NUL can sit in any part.
//!
//! "No text" is a failure, never an empty source: an empty document, an
//! encrypted PDF (not decrypted here), a PDF with no pages, and a scanned
//! or image-only PDF (no text layer) each fail the upload with a message
//! that starts with [`NO_TEXT`] where the cause is "nothing to index".

/// The content types the upload route accepts, as stored on the upload
/// row (lowercased). Anything else is refused at the door with a 415.
pub(crate) const TEXT_PLAIN: &str = "text/plain";
pub(crate) const TEXT_MARKDOWN: &str = "text/markdown";
pub(crate) const TEXT_HTML: &str = "text/html";
pub(crate) const APPLICATION_PDF: &str = "application/pdf";

/// The prefix of every "this document yields nothing to index" failure.
/// The API reports it in the upload's `error`, so a caller can tell a
/// document with no words from a document that could not be read.
pub(crate) const NO_TEXT: &str = "no extractable text";

/// The most pages a PDF may have. Extraction decodes each page's content
/// stream, so this is the CPU bound on the parse; a 4 MiB upload of a
/// larger document is refused, not partially indexed.
pub(crate) const MAX_PDF_PAGES: usize = 500;

/// Pages handed to one `extract_text` call. Between calls the text
/// ceiling is checked, so a pathological document stops after at most
/// this many pages past the ceiling.
const PAGES_PER_CALL: usize = 16;

/// The window in which a PDF's `%PDF-` header must appear. The PDF
/// specification allows junk before the header, within the first 1024
/// bytes.
const PDF_HEADER_WINDOW: usize = 1024;

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

/// The put-time check, on the first part only. Returns the mismatch
/// reason when the bytes seen so far prove they are not the declared
/// type, and `None` otherwise — including when they are too few to tell.
/// A PDF header is only judged once a full sniff window has arrived, so a
/// document split across tiny parts is never refused early by mistake.
pub(crate) fn first_part_mismatch(content_type: &str, first: &[u8]) -> Option<String> {
    match content_type {
        APPLICATION_PDF => (first.len() >= PDF_HEADER_WINDOW && !has_pdf_header(first))
            .then(|| mismatch(content_type, "no %PDF- header in the first 1024 bytes")),
        TEXT_PLAIN | TEXT_MARKDOWN | TEXT_HTML => text_mismatch(content_type, first, true),
        _ => None,
    }
}

/// Reduces `bytes` of `content_type` to text. Every failure is a string
/// for the upload's `error` column: a bad document is the caller's
/// content problem, not an infrastructure fault, and the message should
/// say plainly what could not be read.
pub(crate) fn text_from(content_type: &str, bytes: &[u8]) -> Result<String, String> {
    let text = match content_type {
        TEXT_PLAIN | TEXT_MARKDOWN => utf8(content_type, bytes)?,
        TEXT_HTML => crate::handlers::html_to_text(utf8(content_type, bytes)?.as_str()),
        APPLICATION_PDF => return pdf_text(bytes),
        other => {
            return Err(format!(
                "unsupported content type {other:?}; expected one of {}",
                supported_list()
            ));
        }
    };
    if text.trim().is_empty() {
        return Err(format!("{NO_TEXT}: the document is empty"));
    }
    Ok(text)
}

fn mismatch(content_type: &str, why: &str) -> String {
    format!("content does not match {content_type}: {why}")
}

/// Whether a `%PDF-` header sits in the first [`PDF_HEADER_WINDOW`] bytes.
fn has_pdf_header(bytes: &[u8]) -> bool {
    let window = &bytes[..bytes.len().min(PDF_HEADER_WINDOW)];
    window.windows(5).any(|candidate| candidate == b"%PDF-")
}

/// Why `bytes` cannot be the text type `content_type`, or `None`. A PDF
/// signature, a NUL byte or invalid UTF-8 each rule a text type out. With
/// `partial`, invalid UTF-8 that is only cut short at the end of the slice
/// is allowed: a multi-byte character may continue in the next part.
fn text_mismatch(content_type: &str, bytes: &[u8], partial: bool) -> Option<String> {
    if bytes.starts_with(b"%PDF-") {
        return Some(mismatch(content_type, "the bytes are a PDF"));
    }
    if bytes.contains(&0) {
        return Some(mismatch(
            content_type,
            "the bytes contain NUL, so they are binary",
        ));
    }
    match std::str::from_utf8(bytes) {
        Ok(_) => None,
        Err(err) if partial && err.error_len().is_none() => None,
        Err(_) => Some(mismatch(content_type, "the bytes are not valid UTF-8")),
    }
}

/// Strict UTF-8: an upload that is not text is content the tokenizer
/// would mangle into garbage terms, so it fails the upload instead of
/// being lossily re-encoded into noise.
fn utf8(content_type: &str, bytes: &[u8]) -> Result<String, String> {
    if let Some(why) = text_mismatch(content_type, bytes, false) {
        return Err(why);
    }
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| "not valid UTF-8 text".to_owned())
}

/// PDF text via `lopdf` — pure Rust, wasm-safe, so extraction runs in the
/// Worker isolate too. Pages are read in order, [`PAGES_PER_CALL`] at a
/// time, and reading stops once the text passes the extraction ceiling
/// (the caller then fails the upload on its length).
///
/// Refused, each with a reason: bytes without a `%PDF-` header, an
/// encrypted document (its streams are not decrypted here, so there is no
/// text to read), more than [`MAX_PDF_PAGES`] pages, no pages at all, and
/// a document whose pages yield only whitespace (a scanned or image-only
/// PDF has no text layer, and OCR is out of scope).
fn pdf_text(bytes: &[u8]) -> Result<String, String> {
    if !has_pdf_header(bytes) {
        return Err(format!(
            "unreadable PDF: the bytes do not begin with a %PDF- header (content does not match {APPLICATION_PDF})"
        ));
    }
    // Before lopdf loads anything: `load_mem` itself inflates object
    // streams, so a bomb in an `/ObjStm` is expanded during the load.
    check_inflate_budget(bytes)?;
    let document =
        lopdf::Document::load_mem(bytes).map_err(|err| format!("unreadable PDF: {err}"))?;
    if document.is_encrypted() {
        return Err(format!(
            "{NO_TEXT}: the PDF is encrypted, and encrypted PDFs are not decrypted here; upload an unencrypted copy"
        ));
    }
    let pages: Vec<u32> = document.get_pages().keys().copied().collect();
    if pages.is_empty() {
        return Err(format!("{NO_TEXT}: the PDF has no pages"));
    }
    if pages.len() > MAX_PDF_PAGES {
        return Err(format!(
            "the PDF has {} pages; the maximum is {MAX_PDF_PAGES}",
            pages.len()
        ));
    }

    let ceiling = crate::uploads::MAX_EXTRACTED_TEXT_BYTES;
    let mut text = String::new();
    for chunk in pages.chunks(PAGES_PER_CALL) {
        let read = document
            .extract_text(chunk)
            .map_err(|err| format!("unreadable PDF: {err}"))?;
        text.push_str(&read);
        text.push('\n');
        if text.len() > ceiling {
            break;
        }
    }
    if text.trim().is_empty() {
        return Err(format!(
            "{NO_TEXT}: the PDF has no text layer, so its pages are scanned or image-only"
        ));
    }
    Ok(text)
}

/// The most one Flate stream may inflate to. A content stream of a
/// manual page is kilobytes; 8 MiB is generous for any single stream.
pub(crate) const MAX_STREAM_INFLATED_BYTES: usize = 8 * 1024 * 1024;

/// The most all of a PDF's Flate streams may inflate to, together. Bounds
/// the memory a decompression bomb can take, however many streams it
/// spreads the expansion over.
pub(crate) const MAX_DOCUMENT_INFLATED_BYTES: usize = 32 * 1024 * 1024;

/// Inflates every `stream` body in the raw file, each under
/// [`MAX_STREAM_INFLATED_BYTES`], and refuses the document when one
/// expands past its stream budget or all of them together pass
/// [`MAX_DOCUMENT_INFLATED_BYTES`]. Nothing is stored on refusal.
///
/// The scan runs over the bytes, not the parsed objects, on purpose: a
/// stream body is found by its `stream` keyword, which the PDF syntax
/// requires for every stream (including `ObjStm` object streams, which
/// lopdf inflates while loading). A body that is not zlib data fails
/// its inflate at once and costs nothing toward the budget; a body that
/// is zlib data is counted by what it inflates to, whatever its
/// dictionary claims. The work is bounded by the same budget: no stream
/// is read past its cap, and the scan stops at the document cap.
/// LZW-coded streams are not inflated here (see the README's gaps).
fn check_inflate_budget(bytes: &[u8]) -> Result<(), String> {
    use std::io::Read;

    let mut total = 0_usize;
    let mut from = 0_usize;
    while let Some(offset) = bytes[from..]
        .windows(6)
        .position(|window| window == b"stream")
    {
        let keyword = from + offset;
        from = keyword + 6;
        // `endstream` is not a body.
        if keyword >= 3 && &bytes[keyword - 3..keyword] == b"end" {
            continue;
        }
        // The body follows the keyword's end-of-line: CRLF, LF or CR.
        let mut body = from;
        match bytes.get(body) {
            Some(b'\r') => {
                body += 1;
                if bytes.get(body) == Some(&b'\n') {
                    body += 1;
                }
            }
            Some(b'\n') => body += 1,
            _ => {}
        }
        let mut inflated = Vec::new();
        // The read stops at the cap plus one byte, so an over-budget
        // stream is detected without inflating it in full. A decode error
        // keeps whatever was produced: partial output still counts.
        let cap = u64::try_from(MAX_STREAM_INFLATED_BYTES + 1).unwrap_or(u64::MAX);
        let _ = flate2::read::ZlibDecoder::new(&bytes[body..])
            .take(cap)
            .read_to_end(&mut inflated);
        if inflated.len() > MAX_STREAM_INFLATED_BYTES {
            return Err(format!(
                "document too large when decompressed: a stream inflates past {} MiB",
                MAX_STREAM_INFLATED_BYTES / (1024 * 1024)
            ));
        }
        total += inflated.len();
        if total > MAX_DOCUMENT_INFLATED_BYTES {
            return Err(format!(
                "document too large when decompressed: its streams inflate past {} MiB in total",
                MAX_DOCUMENT_INFLATED_BYTES / (1024 * 1024)
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_stream_inflating_past_its_cap_is_refused() {
        let pdf = pdf_with_streams(&[vec![0_u8; MAX_STREAM_INFLATED_BYTES + 1]]);
        let err = text_from(APPLICATION_PDF, &pdf).expect_err("bomb");
        assert!(
            err.contains("document too large when decompressed"),
            "{err}"
        );
    }

    #[test]
    fn many_streams_past_the_document_cap_are_refused() {
        // Each stream is under its own cap; together they pass the document's.
        let streams = vec![vec![0_u8; 7 * 1024 * 1024]; 5];
        let pdf = pdf_with_streams(&streams);
        let err = text_from(APPLICATION_PDF, &pdf).expect_err("bomb");
        assert!(err.contains("in total"), "{err}");
    }

    #[test]
    fn streams_within_both_budgets_pass_the_scan() {
        let streams = vec![vec![b'a'; 1024 * 1024]; 3];
        let pdf = pdf_with_streams(&streams);
        check_inflate_budget(&pdf).expect("within budget");
    }

    /// A PDF with one unreferenced Flate stream per entry of `streams`,
    /// written through lopdf's own compressor so the bytes are real zlib.
    fn pdf_with_streams(streams: &[Vec<u8>]) -> Vec<u8> {
        use lopdf::dictionary;
        let mut doc = lopdf::Document::with_version("1.4");
        for body in streams {
            doc.add_object(lopdf::Stream::new(dictionary! {}, body.clone()));
        }
        let pages_id = doc.add_object(dictionary! {
            "Type" => "Pages",
            "Kids" => lopdf::Object::Array(Vec::new()),
            "Count" => 0,
        });
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

    #[test]
    fn a_pdf_declared_as_text_is_a_mismatch() {
        let err = text_from(TEXT_PLAIN, b"%PDF-1.4\n%binary").expect_err("pdf as text");
        assert!(
            err.starts_with("content does not match text/plain"),
            "{err}"
        );
    }

    #[test]
    fn a_nul_byte_makes_a_text_upload_binary() {
        let err = text_from(TEXT_MARKDOWN, b"ab\0cd").expect_err("binary");
        assert!(err.contains("NUL"), "{err}");
    }

    #[test]
    fn empty_and_tag_only_documents_have_no_extractable_text() {
        let blank = text_from(TEXT_PLAIN, b" \n\t ").expect_err("blank");
        assert!(blank.starts_with(NO_TEXT), "{blank}");
        let tags = text_from(TEXT_HTML, b"<html><body></body></html>").expect_err("tags only");
        assert!(tags.starts_with(NO_TEXT), "{tags}");
    }

    #[test]
    fn the_put_time_check_refuses_only_what_the_first_part_proves() {
        // A PDF with no header, once a whole sniff window has arrived.
        assert!(first_part_mismatch(APPLICATION_PDF, &[b'x'; 2000]).is_some());
        // Too few bytes to judge a header: not refused early.
        assert!(first_part_mismatch(APPLICATION_PDF, b"%PD").is_none());
        // A real header, however far in the window it sits.
        let mut pdf = vec![b' '; 900];
        pdf.extend_from_slice(b"%PDF-1.7");
        assert!(first_part_mismatch(APPLICATION_PDF, &pdf).is_none());
        // Text: a PDF signature or a NUL refuses; a character cut at the
        // part's end does not.
        assert!(first_part_mismatch(TEXT_PLAIN, b"%PDF-1.4").is_some());
        assert!(first_part_mismatch(TEXT_PLAIN, b"a\0b").is_some());
        let split = "héllo".as_bytes()[..2].to_vec();
        assert!(first_part_mismatch(TEXT_PLAIN, &split).is_none());
        assert!(
            text_from(TEXT_PLAIN, &split).is_err(),
            "the whole document must be valid"
        );
    }

    /// A one-page PDF whose page shows `text`, built with the same lopdf
    /// API the integration suite uses, then serialized and extracted back
    /// to text. The heavy PDF flows are covered by the integration suite;
    /// this pins the dispatcher to the right branch.
    #[test]
    fn a_pdf_yields_its_text() {
        let pdf = pdf_with_pages(&[vec![show("lopdf and the support module")]], false);
        let text = text_from(APPLICATION_PDF, &pdf).expect("pdf text");
        assert!(text.contains("lopdf and the support module"), "{text:?}");
    }

    #[test]
    fn a_non_pdf_fails_with_a_named_error() {
        let err = text_from(APPLICATION_PDF, b"definitely not a pdf").expect_err("not a pdf");
        assert!(err.contains("unreadable PDF"), "{err}");
    }

    #[test]
    fn a_pdf_with_no_text_operations_is_scanned_not_empty_text() {
        let pdf = pdf_with_pages(&[vec![]], false);
        let err = text_from(APPLICATION_PDF, &pdf).expect_err("no text layer");
        assert!(err.starts_with(NO_TEXT), "{err}");
        assert!(err.contains("scanned"), "{err}");
    }

    #[test]
    fn an_encrypted_pdf_is_refused_before_any_page_is_read() {
        let pdf = pdf_with_pages(&[vec![show("secret")]], true);
        let err = text_from(APPLICATION_PDF, &pdf).expect_err("encrypted");
        assert!(
            err.starts_with(NO_TEXT) && err.contains("encrypted"),
            "{err}"
        );
    }

    #[test]
    fn a_pdf_with_no_pages_has_no_extractable_text() {
        let pdf = pdf_with_pages(&[], false);
        let err = text_from(APPLICATION_PDF, &pdf).expect_err("no pages");
        assert!(
            err.starts_with(NO_TEXT) && err.contains("no pages"),
            "{err}"
        );
    }

    fn show(text: &str) -> lopdf::content::Operation {
        lopdf::content::Operation::new("Tj", vec![lopdf::Object::string_literal(text)])
    }

    /// A PDF with one page per entry of `pages`, each page's content
    /// stream made of those operations. `encrypted` adds an `/Encrypt`
    /// entry to the trailer, which is all `is_encrypted` looks at.
    fn pdf_with_pages(pages: &[Vec<lopdf::content::Operation>], encrypted: bool) -> Vec<u8> {
        // The macro must be imported, not path-invoked: lopdf's
        // trailing-comma arm recurses as a bare `dictionary!`, which only
        // resolves when the macro is in scope.
        use lopdf::content::Content;
        use lopdf::dictionary;

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
            "Count" => 0,
        });
        let mut kids = Vec::new();
        for operations in pages {
            let content = Content {
                operations: operations.clone(),
            };
            let content_id = doc.add_object(lopdf::Stream::new(
                dictionary! {},
                content.encode().expect("content encodes"),
            ));
            let leaf_id = doc.add_object(dictionary! {
                "Type" => "Page",
                "Parent" => pages_id,
                "Contents" => content_id,
                "Resources" => resources_id,
            });
            kids.push(lopdf::Object::Reference(leaf_id));
        }
        if let lopdf::Object::Dictionary(dict) = doc.objects.get_mut(&pages_id).expect("pages") {
            dict.set("Kids", lopdf::Object::Array(kids));
            dict.set("Count", i64::try_from(pages.len()).expect("page count"));
        }
        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        doc.trailer.set("Root", catalog_id);
        if encrypted {
            let encrypt_id = doc.add_object(dictionary! {
                "Filter" => "Standard",
                "V" => 1,
                "R" => 2,
                "O" => lopdf::Object::string_literal(vec![0_u8; 32]),
                "U" => lopdf::Object::string_literal(vec![0_u8; 32]),
                "P" => -4,
            });
            doc.trailer.set("Encrypt", encrypt_id);
        }
        doc.compress();
        let mut bytes = Vec::new();
        doc.save_to(&mut bytes).expect("serialises");
        bytes
    }
}
