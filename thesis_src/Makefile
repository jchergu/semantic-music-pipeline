CHAPTERS := $(sort $(wildcard [0-9][0-9]-*.md))
BUILD_DIR := ../build
OUT := $(BUILD_DIR)/thesis.docx
PYTHON ?= python3
LOCK := $(BUILD_DIR)/.~lock.thesis.docx\#

.PHONY: all clean

all: $(OUT)

# Page 1 lives in titlepage.docx, not in the Markdown -- see
# merge_frontmatter.py for why, and edit it there, never in $(OUT).
$(OUT): $(CHAPTERS) reference.docx titlepage.docx merge_frontmatter.py update_toc.py
	@if [ -e "$(LOCK)" ]; then \
	    echo "ERROR: $(OUT) is open in LibreOffice. Close it first --"; \
	    echo "       it is build output, and this rule would overwrite"; \
	    echo "       whatever you have edited there."; \
	    exit 1; \
	fi
	mkdir -p $(BUILD_DIR)
	pandoc $(CHAPTERS) --reference-doc=reference.docx --toc -o $(OUT)
	$(PYTHON) merge_frontmatter.py $(OUT) titlepage.docx
	$(PYTHON) update_toc.py $(OUT)

clean:
	rm -rf $(BUILD_DIR)
