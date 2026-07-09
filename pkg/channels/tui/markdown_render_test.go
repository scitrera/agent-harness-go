package tui

import (
	"strings"
	"testing"
)

func TestModelRenderRow_rendersAssistantMarkdown(t *testing.T) {
	// Given
	m := model{width: 80}
	row := chatRow{Kind: rowAssistant, Text: "This is **bold** and *emphasis*."}

	// When
	rendered := m.renderRow(row)

	// Then
	if !strings.Contains(rendered, "bold") || !strings.Contains(rendered, "emphasis") {
		t.Fatalf("rendered markdown is missing text: %q", rendered)
	}
	if strings.Contains(rendered, "**bold**") || strings.Contains(rendered, "*emphasis*") {
		t.Fatalf("assistant markdown markers should be rendered, not shown literally: %q", rendered)
	}
}

func TestModelRenderRow_keepsPlainAssistantTextUnchanged(t *testing.T) {
	// Given
	m := model{width: 80}
	row := chatRow{Kind: rowAssistant, Text: "hello from agent"}

	// When
	rendered := m.renderRow(row)

	// Then
	if !strings.Contains(rendered, "hello from agent") {
		t.Fatalf("plain assistant text should remain contiguous: %q", rendered)
	}
}

func TestModelRenderRow_rendersAssistantMarkdownBlocks(t *testing.T) {
	// Given
	m := model{width: 96}
	row := chatRow{Kind: rowAssistant, Text: strings.Join([]string{
		"## Plan",
		"",
		"- first",
		"- second",
		"",
		"| Name | Value |",
		"| --- | --- |",
		"| alpha | 1 |",
	}, "\n")}

	// When
	rendered := m.renderRow(row)

	// Then
	for _, want := range []string{"Plan", "first", "second", "Name", "Value", "alpha", "1"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered markdown is missing %q: %q", want, rendered)
		}
	}
	if strings.Contains(rendered, "| --- | --- |") {
		t.Fatalf("table separator should be rendered, not shown literally: %q", rendered)
	}
}

func TestModelRenderRow_keepsUserMarkdownLiteral(t *testing.T) {
	// Given
	m := model{width: 80}
	row := chatRow{Kind: rowUser, Text: "Please keep **this** literal."}

	// When
	rendered := m.renderRow(row)

	// Then
	if !strings.Contains(rendered, "**this**") {
		t.Fatalf("user text should remain literal: %q", rendered)
	}
}
