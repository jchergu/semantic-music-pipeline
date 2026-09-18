#!/usr/bin/env python3
"""Compute the table of contents pandoc leaves as an unevaluated field.

Pandoc's docx writer emits the TOC as a real Word field (an <w:sdt> wrapping
a TOC field code, spliced into place by merge_frontmatter.py) rather than
pre-rendered text. No cached result is baked in, so it opens blank until
whatever application opens the document runs its own "update field" pass --
Word does this inconsistently depending on user settings, and a headless
`soffice --convert-to pdf` never does it at all. Contributors checking the
build by converting to PDF from the command line would see an empty page 2
and reasonably conclude the TOC was broken, when the .docx itself was fine
and just unevaluated.

This script starts a throwaway, headless LibreOffice instance (its own
temp user profile, so it cannot collide with a real interactive session or
with the Makefile's own lock check on `build/thesis.docx`), opens the built
document, updates every document index it contains (the TOC is exposed as
one), and saves the result back in place -- so the shipped .docx already
carries a computed TOC no matter what opens it next.

It does this in a loop of separate LibreOffice sessions, each reloading
the file fresh from disk, until the document's full text stops changing
between two consecutive sessions. Every one of these fields starts blank;
filling one in adds pages, which pushes every page number after it
forward -- including the ones its own just-written entries now claim,
since they were computed against the pre-fill layout. Updating twice back
to back *within the same session* does not fix this (tested: every
chapter from 2 onward still came out one page short). Reloading between
passes does. With three such fields chained one after another (the TOC,
List of Figures, List of Tables), each one's fill-in also shifts every
field after it, so a fixed pass count does not reliably converge either --
two passes was enough for the TOC alone, but left the List of Figures and
List of Tables each one page short once both existed, and the total page
count had already stopped changing by then (a real case caught by
comparing full text, not by trusting page count as a proxy for it -- both
were checked against the actual rendered PDF, not assumed). Looping on
text equality instead directly targets the thing that must stop changing:
the page-number digits are literal characters inside each field's cached
result, not a display-only computation, so any field still drifting shows
up as a text difference. This mirrors what opening the file, updating
fields, saving, and reopening it repeatedly would do by hand; it is just
automated here.

    usage: update_toc.py <built.docx>
"""

import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile
from pathlib import Path


def main(docx_path):
    docx_path = Path(docx_path).resolve()
    previous_text = None
    for i in range(1, 7):
        text, pages = _update_once(docx_path)
        print(f"  pass {i}: document is now {pages} page(s)")
        if text == previous_text:
            break
        previous_text = text
    else:
        print("  warning: full text did not stabilize after 6 passes; "
              "using the last result")
    strip_list_page_numbers(docx_path)
    # The strip/spacing step above runs outside LibreOffice, directly on
    # the saved XML, and changes the document's page count (the whole
    # point of the spacing knob). The main TOC is still a live field at
    # this point and its cached page numbers were computed before that
    # change, so every one of them is now off by however many pages the
    # List of Figures/Tables grew by. One more real pass re-resolves it
    # against the now-final layout.
    _, pages = _update_once(docx_path)
    print(f"  final pass: document is now {pages} page(s)")


def _update_once(docx_path):
    pipe_name = f"thesis-toc-{uuid.uuid4().hex}"
    with tempfile.TemporaryDirectory(prefix="lo-profile-") as profile_dir:
        proc = subprocess.Popen([
            "soffice", "--headless", "--invisible", "--nologo",
            "--nofirststartwizard",
            f"-env:UserInstallation=file://{profile_dir}",
            f"--accept=pipe,name={pipe_name};urp;",
        ])
        try:
            return _update(docx_path, pipe_name)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


def _update(docx_path, pipe_name):
    import uno
    from com.sun.star.beans import PropertyValue

    local_context = uno.getComponentContext()
    resolver = local_context.ServiceManager.createInstanceWithContext(
        "com.sun.star.bridge.UnoUrlResolver", local_context)

    ctx = None
    for _ in range(30):
        try:
            ctx = resolver.resolve(
                f"uno:pipe,name={pipe_name};urp;StarOffice.ComponentContext")
            break
        except Exception:
            time.sleep(1)
    if ctx is None:
        sys.exit("error: could not connect to the headless LibreOffice instance")

    smgr = ctx.ServiceManager
    desktop = smgr.createInstanceWithContext("com.sun.star.frame.Desktop", ctx)

    def prop(name, value):
        p = PropertyValue()
        p.Name = name
        p.Value = value
        return p

    url = uno.systemPathToFileUrl(str(docx_path))
    doc = desktop.loadComponentFromURL(url, "_blank", 0, (prop("Hidden", True),))
    try:
        # getDocumentIndexes().update() looked right, and does work -- but a
        # style-based TOC field (used for the List of Figures/Tables, since
        # their captions aren't SEQ-numbered Word captions) loses the
        # <w:sdt>/docPartGallery wrapper that marks it as an "index" when
        # LibreOffice saves it back out after its first update. On the next
        # reload it is still a perfectly valid field, just no longer one
        # getDocumentIndexes() finds, so it silently stopped being updated
        # on this script's second pass while the real TOC kept converging.
        # Dispatching .uno:UpdateAll is what Tools > Update > Update All
        # does by hand: it walks every field in the document by content,
        # not by which collection happens to expose it, so it is not
        # sensitive to that wrapper being dropped.
        controller = doc.getCurrentController()
        frame = controller.getFrame()
        dispatcher = smgr.createInstanceWithContext(
            "com.sun.star.frame.DispatchHelper", ctx)
        dispatcher.executeDispatch(frame, ".uno:UpdateAll", "", 0, ())
        doc.store()

        full_text = doc.getText().getString()
        view_cursor = controller.getViewCursor()
        view_cursor.jumpToLastPage()
        page_count = view_cursor.getPage()
    finally:
        doc.close(False)
    desktop.terminate()
    return full_text, page_count


# Extra space-after on each List of Figures/Tables entry, beyond the TOC1
# style's own default -- purely a page-count knob (see the Makefile), not a
# correctness fix like the rest of this file. Tuned separately per list: an
# equal value for both (tried first) gave List of Figures a well-balanced
# 7/9 split across its two pages but left List of Tables with only 2 of its
# 16 entries spilling onto an otherwise-blank third page, which read as a
# mistake rather than intentional spacing. Giving List of Tables a smaller
# bump keeps it on one page; List of Figures makes up the resulting page
# short-fall.
LIST_OF_FIGURES_SPACING_TWIPS = 400
LIST_OF_TABLES_SPACING_TWIPS = 160


def strip_list_page_numbers(docx_path):
    """Remove the page-number run LibreOffice adds to every List of
    Figures/Tables entry despite the field's `\\n` (no page numbers)
    switch -- confirmed present in the field code at every stage up to
    LibreOffice's own resolution of it, so this is a real limitation of
    that field variant there, not a switch this build is failing to set.
    The numbers it computes are wrong beyond the first few entries (checked
    against the actual rendered PDF: correct for entry 1, four pages off by
    entry 3, low double digits off by the middle of the list), so leaving
    them in would ship numbers that look authoritative and are not.

    Runs directly on the saved XML rather than through LibreOffice, since
    by this point the fields have already been flattened to plain
    TOC1-styled paragraphs (see the update loop above) -- there is no live
    field left to reconfigure, only text to trim.
    """
    with zipfile.ZipFile(docx_path) as z:
        parts = {name: z.read(name) for name in z.namelist()}
    xml = parts["word/document.xml"].decode("utf-8")

    lof_start = xml.find(">List of Figures<")
    lot_start = xml.find(">List of Tables<")
    end = xml.find("Heading1", lot_start)
    if lof_start == -1 or lot_start == -1 or end == -1:
        print("  warning: could not find the List of Figures/Tables range; "
              "leaving page numbers as-is")
        return

    before = xml[:lof_start]
    lof_span = xml[lof_start:lot_start]
    lot_span = xml[lot_start:end]
    after = xml[end:]

    n = n_spaced = 0
    spans = []
    for span, spacing in ((lof_span, LIST_OF_FIGURES_SPACING_TWIPS),
                          (lot_span, LIST_OF_TABLES_SPACING_TWIPS)):
        span, stripped = re.subn(r"<w:tab/><w:t>\d+</w:t></w:r></w:hyperlink>",
                                  "</w:r></w:hyperlink>", span)
        span, spaced = re.subn(
            r'(<w:pStyle w:val="TOC1"/><w:tabs>.*?</w:tabs>)(<w:rPr>)',
            rf'\1<w:spacing w:after="{spacing}"/>\2', span)
        n += stripped
        n_spaced += spaced
        spans.append(span)
    if n == 0:
        print("  note: no List of Figures/Tables page-number runs found "
              "to strip (already clean)")
        return
    parts["word/document.xml"] = (before + spans[0] + spans[1] + after).encode("utf-8")

    temporary = Path(str(docx_path) + ".tmp")
    with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in parts.items():
            z.writestr(name, data)
    shutil.move(temporary, docx_path)
    print(f"  stripped {n} page-number run(s) and widened spacing after "
          f"{n_spaced} entries in the List of Figures/Tables")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__.strip().splitlines()[-1].strip())
    main(sys.argv[1])
