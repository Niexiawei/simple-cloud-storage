package pathx

import "testing"

func TestPrefix(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"app":              "app",
		" /apps/demo/ ":    "apps/demo",
		"apps/demo/v1":     "apps/demo/v1",
		"目录/测试":            "目录/测试",
		"/ app /":          "app",
		" / /app/ / ":      "app",
		"app/ inner /file": "app/ inner /file",
	}
	for input, want := range tests {
		input, want := input, want
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			got, err := Prefix(input)
			if err != nil || got != want {
				t.Fatalf("Prefix(%q) = %q, %v; want %q", input, got, err, want)
			}
			again, err := Prefix(got)
			if err != nil || again != got {
				t.Fatalf("non-idempotent prefix: %q -> %q: %v", got, again, err)
			}
		})
	}
}

func TestInvalidPaths(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "/", " ", "/ /", "a//b", "a/./b", "a/../b", "a\\b", "a/\x00/b", "a/\n/b", "a\u0085b", "a\xffb"} {
		if _, err := Prefix(value); err == nil {
			t.Errorf("Prefix(%q) unexpectedly succeeded", value)
		}
	}
	for _, value := range []string{"", "/a", "a/", "a//b", ".", "..", "a/../b", "a\\b", "a\x00b", "a\u009fb", "a\xffb"} {
		if _, err := Key(value); err == nil {
			t.Errorf("Key(%q) unexpectedly succeeded", value)
		}
	}
}

func TestJoin(t *testing.T) {
	t.Parallel()
	got, err := Join("/apps/demo/", "documents/a.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if want := "apps/demo/documents/a.mp4"; got != want {
		t.Fatalf("Join() = %q, want %q", got, want)
	}
	if _, err := Join("app", "../outside"); err == nil {
		t.Fatal("Join() accepted path traversal")
	}
}
