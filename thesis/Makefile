CHAPTERS := $(sort $(wildcard [0-9][0-9]-*.md))
BUILD_DIR := ../build
OUT := $(BUILD_DIR)/thesis.docx

.PHONY: all clean

all: $(OUT)

$(OUT): $(CHAPTERS) reference.docx
	mkdir -p $(BUILD_DIR)
	pandoc $(CHAPTERS) --reference-doc=reference.docx --toc -o $(OUT)

clean:
	rm -rf $(BUILD_DIR)
