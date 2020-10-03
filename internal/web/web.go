// Install the tool
//go:generate go build -o ../../bin/go-bindata github.com/go-bindata/go-bindata/v3/go-bindata

// Generate the assets
//go:generate ../../bin/go-bindata -fs -pkg web -prefix "public/" -o ./bindata.go -tags=!dev public/

// Generate stub versions of the assets for dev
//go:generate ../../bin/go-bindata -fs -pkg web -prefix "public/" -o ./bindata-dev.go -tags=dev -dev public/

package web
