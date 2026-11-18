// Package frontend embeds the built web app (frontend/dist, produced by
// `npm run build`). The directory is committed with only a .gitkeep, so the
// Go code builds without Node; the api service then serves a hint instead.
package frontend

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

// Files returns the built app with index.html at the root.
func Files() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err) // the directory is embedded at compile time
	}
	return sub
}
