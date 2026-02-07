package assets

import _ "embed"

//go:embed openapi.yaml
var OpenApiData []byte

//go:embed menus.yaml
var MenusData []byte

//go:embed descriptor.bin
var DescriptorData []byte
