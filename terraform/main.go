// Strata Terraform Provider
// Enables infrastructure-as-code management of Strata resources

package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
	"github.com/strata/terraform-provider-strata/strata"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: strata.Provider,
	})
}
