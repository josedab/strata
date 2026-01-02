// Provider configuration for Strata

package strata

import (
	"context"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

// Provider returns the Strata Terraform provider
func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"endpoint": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("STRATA_ENDPOINT", "http://localhost:9000"),
				Description: "The Strata server endpoint",
			},
			"access_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("STRATA_ACCESS_KEY", nil),
				Description: "Access key for authentication",
			},
			"secret_key": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("STRATA_SECRET_KEY", nil),
				Description: "Secret key for authentication",
			},
			"token": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("STRATA_TOKEN", nil),
				Description: "API token for authentication",
			},
			"region": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("STRATA_REGION", "us-east-1"),
				Description: "Default region for resources",
			},
			"tls_enabled": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Enable TLS for connections",
			},
			"tls_skip_verify": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Skip TLS certificate verification (insecure)",
			},
		},
		ResourcesMap: map[string]*schema.Resource{
			"strata_bucket":            resourceBucket(),
			"strata_bucket_policy":     resourceBucketPolicy(),
			"strata_bucket_versioning": resourceBucketVersioning(),
			"strata_bucket_lifecycle":  resourceBucketLifecycle(),
			"strata_bucket_cors":       resourceBucketCORS(),
			"strata_object":            resourceObject(),
			"strata_user":              resourceUser(),
			"strata_access_key":        resourceAccessKey(),
		},
		DataSourcesMap: map[string]*schema.Resource{
			"strata_bucket":  dataSourceBucket(),
			"strata_buckets": dataSourceBuckets(),
			"strata_object":  dataSourceObject(),
			"strata_cluster": dataSourceCluster(),
		},
		ConfigureContextFunc: providerConfigure,
	}
}

func providerConfigure(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	config := &Config{
		Endpoint:      d.Get("endpoint").(string),
		AccessKey:     d.Get("access_key").(string),
		SecretKey:     d.Get("secret_key").(string),
		Token:         d.Get("token").(string),
		Region:        d.Get("region").(string),
		TLSEnabled:    d.Get("tls_enabled").(bool),
		TLSSkipVerify: d.Get("tls_skip_verify").(bool),
	}

	client, err := NewClient(config)
	if err != nil {
		return nil, diag.FromErr(err)
	}

	return client, nil
}
