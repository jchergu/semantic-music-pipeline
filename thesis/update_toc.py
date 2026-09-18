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

It does this twice, in two separate LibreOffice sessions each reloading
the file fresh from disk. The TOC starts as a single blank field; filling
it in adds several pages of content, which pushes every page number after
it forward -- including the ones the very entries just written now claim,
since they were computed against the pre-fill layout. Doing two update()
calls back to back *within the same session* does not fix this (tested:
every chapter from 2 onward still came out one page short). Reloading
between passes does -- the second session lays the document out fresh
with the now-correct TOC length already present on disk, and converges.
This mirrors what opening the file, updating the field, saving, and
reopening it would do by hand; it is just automated here.

    usage: update_toc.py <built.docx>
"""

import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path


def main(docx_path):
    docx_path = Path(docx_path).resolve()
    for _ in range(2):
        _update_once(docx_path)


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
            _update(docx_path, pipe_name)
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
        indexes = doc.getDocumentIndexes()
        for i in range(indexes.getCount()):
            indexes.getByIndex(i).update()
        print(f"  updated {indexes.getCount()} document index(es) "
              f"(table of contents) in {docx_path.name}")
        doc.store()
    finally:
        doc.close(False)
    desktop.terminate()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__.strip().splitlines()[-1].strip())
    main(sys.argv[1])
