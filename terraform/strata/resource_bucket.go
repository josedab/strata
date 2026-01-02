// Bucket resource for Terraform

package strata

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceBucket() *schema.Resource {
	return &schema.Resource{
		Description: "Manages a Strata bucket",

		CreateContext: resourceBucketCreate,
		ReadContext:   resourceBucketRead,
		UpdateContext: resourceBucketUpdate,
		DeleteContext: resourceBucketDelete,

		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the bucket",
			},
			"region": {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "The region for the bucket",
			},
			"versioning": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Enable versioning for the bucket",
			},
			"encryption": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "Enable encryption for the bucket",
			},
			"tags": {
				Type:        schema.TypeMap,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Tags for the bucket",
			},
			"force_destroy": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Delete all objects when destroying the bucket",
			},
			// Computed attributes
			"created_at": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Creation timestamp",
			},
			"object_count": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Number of objects in the bucket",
			},
			"total_size": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Total size of objects in bytes",
			},
		},
	}
}

func resourceBucketCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	name := d.Get("name").(string)

	req := map[string]interface{}{
		"name":       name,
		"versioning": d.Get("versioning").(bool),
		"encryption": d.Get("encryption").(bool),
	}

	if v, ok := d.GetOk("region"); ok {
		req["region"] = v.(string)
	}

	if v, ok := d.GetOk("tags"); ok {
		req["tags"] = v.(map[string]interface{})
	}

	var bucket Bucket
	err := client.Post(ctx, "/buckets", req, &bucket)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to create bucket: %w", err))
	}

	d.SetId(name)

	return resourceBucketRead(ctx, d, m)
}

func resourceBucketRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	name := d.Id()

	var bucket Bucket
	err := client.Get(ctx, "/buckets/"+name, &bucket)
	if err != nil {
		// Check if not found
		d.SetId("")
		return nil
	}

	d.Set("name", bucket.Name)
	d.Set("region", bucket.Region)
	d.Set("versioning", bucket.Versioning)
	d.Set("encryption", bucket.Encryption)
	d.Set("created_at", bucket.CreatedAt.Format("2006-01-02T15:04:05Z"))
	d.Set("object_count", bucket.ObjectCount)
	d.Set("total_size", bucket.TotalSize)

	if bucket.Tags != nil {
		d.Set("tags", bucket.Tags)
	}

	return nil
}

func resourceBucketUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	name := d.Id()

	if d.HasChanges("versioning", "encryption", "tags") {
		req := map[string]interface{}{}

		if d.HasChange("versioning") {
			req["versioning"] = d.Get("versioning").(bool)
		}
		if d.HasChange("encryption") {
			req["encryption"] = d.Get("encryption").(bool)
		}
		if d.HasChange("tags") {
			req["tags"] = d.Get("tags").(map[string]interface{})
		}

		err := client.Put(ctx, "/buckets/"+name, req, nil)
		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to update bucket: %w", err))
		}
	}

	return resourceBucketRead(ctx, d, m)
}

func resourceBucketDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	name := d.Id()
	forceDestroy := d.Get("force_destroy").(bool)

	path := "/buckets/" + name
	if forceDestroy {
		path += "?force=true"
	}

	err := client.Delete(ctx, path)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete bucket: %w", err))
	}

	d.SetId("")

	return nil
}

func resourceBucketPolicy() *schema.Resource {
	return &schema.Resource{
		Description: "Manages a Strata bucket policy",

		CreateContext: resourceBucketPolicyCreate,
		ReadContext:   resourceBucketPolicyRead,
		UpdateContext: resourceBucketPolicyUpdate,
		DeleteContext: resourceBucketPolicyDelete,

		Schema: map[string]*schema.Schema{
			"bucket": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The bucket name",
			},
			"policy": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The policy document (JSON)",
			},
		},
	}
}

func resourceBucketPolicyCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Get("bucket").(string)
	policy := d.Get("policy").(string)

	req := map[string]interface{}{
		"policy": policy,
	}

	err := client.Put(ctx, "/buckets/"+bucket+"/policy", req, nil)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(bucket)

	return nil
}

func resourceBucketPolicyRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Id()

	var result map[string]interface{}
	err := client.Get(ctx, "/buckets/"+bucket+"/policy", &result)
	if err != nil {
		d.SetId("")
		return nil
	}

	d.Set("bucket", bucket)
	if policy, ok := result["policy"].(string); ok {
		d.Set("policy", policy)
	}

	return nil
}

func resourceBucketPolicyUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	return resourceBucketPolicyCreate(ctx, d, m)
}

func resourceBucketPolicyDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Id()

	err := client.Delete(ctx, "/buckets/"+bucket+"/policy")
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return nil
}

func resourceBucketVersioning() *schema.Resource {
	return &schema.Resource{
		Description: "Manages bucket versioning configuration",

		CreateContext: resourceBucketVersioningSet,
		ReadContext:   resourceBucketVersioningRead,
		UpdateContext: resourceBucketVersioningSet,
		DeleteContext: resourceBucketVersioningDelete,

		Schema: map[string]*schema.Schema{
			"bucket": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The bucket name",
			},
			"enabled": {
				Type:        schema.TypeBool,
				Required:    true,
				Description: "Enable versioning",
			},
			"mfa_delete": {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Require MFA for delete operations",
			},
		},
	}
}

func resourceBucketVersioningSet(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Get("bucket").(string)

	req := map[string]interface{}{
		"enabled":    d.Get("enabled").(bool),
		"mfa_delete": d.Get("mfa_delete").(bool),
	}

	err := client.Put(ctx, "/buckets/"+bucket+"/versioning", req, nil)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(bucket)

	return nil
}

func resourceBucketVersioningRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Id()

	var result map[string]interface{}
	err := client.Get(ctx, "/buckets/"+bucket+"/versioning", &result)
	if err != nil {
		d.SetId("")
		return nil
	}

	d.Set("bucket", bucket)
	d.Set("enabled", result["enabled"])
	d.Set("mfa_delete", result["mfa_delete"])

	return nil
}

func resourceBucketVersioningDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Id()

	req := map[string]interface{}{
		"enabled": false,
	}

	err := client.Put(ctx, "/buckets/"+bucket+"/versioning", req, nil)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return nil
}

func resourceBucketLifecycle() *schema.Resource {
	return &schema.Resource{
		Description: "Manages bucket lifecycle rules",

		CreateContext: resourceBucketLifecycleSet,
		ReadContext:   resourceBucketLifecycleRead,
		UpdateContext: resourceBucketLifecycleSet,
		DeleteContext: resourceBucketLifecycleDelete,

		Schema: map[string]*schema.Schema{
			"bucket": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The bucket name",
			},
			"rule": {
				Type:        schema.TypeList,
				Required:    true,
				Description: "Lifecycle rules",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "Rule ID",
						},
						"enabled": {
							Type:        schema.TypeBool,
							Required:    true,
							Description: "Whether the rule is enabled",
						},
						"prefix": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "Object key prefix filter",
						},
						"expiration_days": {
							Type:        schema.TypeInt,
							Optional:    true,
							Description: "Days until expiration",
						},
						"transition_days": {
							Type:        schema.TypeInt,
							Optional:    true,
							Description: "Days until transition",
						},
						"transition_storage_class": {
							Type:        schema.TypeString,
							Optional:    true,
							Description: "Target storage class for transition",
						},
					},
				},
			},
		},
	}
}

func resourceBucketLifecycleSet(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Get("bucket").(string)
	rules := d.Get("rule").([]interface{})

	req := map[string]interface{}{
		"rules": rules,
	}

	err := client.Put(ctx, "/buckets/"+bucket+"/lifecycle", req, nil)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(bucket)

	return nil
}

func resourceBucketLifecycleRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Id()

	var result map[string]interface{}
	err := client.Get(ctx, "/buckets/"+bucket+"/lifecycle", &result)
	if err != nil {
		d.SetId("")
		return nil
	}

	d.Set("bucket", bucket)
	d.Set("rule", result["rules"])

	return nil
}

func resourceBucketLifecycleDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Id()

	err := client.Delete(ctx, "/buckets/"+bucket+"/lifecycle")
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return nil
}

func resourceBucketCORS() *schema.Resource {
	return &schema.Resource{
		Description: "Manages bucket CORS configuration",

		CreateContext: resourceBucketCORSSet,
		ReadContext:   resourceBucketCORSRead,
		UpdateContext: resourceBucketCORSSet,
		DeleteContext: resourceBucketCORSDelete,

		Schema: map[string]*schema.Schema{
			"bucket": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The bucket name",
			},
			"rule": {
				Type:        schema.TypeList,
				Required:    true,
				Description: "CORS rules",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"allowed_origins": {
							Type:        schema.TypeList,
							Required:    true,
							Elem:        &schema.Schema{Type: schema.TypeString},
							Description: "Allowed origins",
						},
						"allowed_methods": {
							Type:        schema.TypeList,
							Required:    true,
							Elem:        &schema.Schema{Type: schema.TypeString},
							Description: "Allowed HTTP methods",
						},
						"allowed_headers": {
							Type:        schema.TypeList,
							Optional:    true,
							Elem:        &schema.Schema{Type: schema.TypeString},
							Description: "Allowed headers",
						},
						"expose_headers": {
							Type:        schema.TypeList,
							Optional:    true,
							Elem:        &schema.Schema{Type: schema.TypeString},
							Description: "Headers to expose",
						},
						"max_age_seconds": {
							Type:        schema.TypeInt,
							Optional:    true,
							Description: "Max age for preflight cache",
						},
					},
				},
			},
		},
	}
}

func resourceBucketCORSSet(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Get("bucket").(string)
	rules := d.Get("rule").([]interface{})

	req := map[string]interface{}{
		"rules": rules,
	}

	err := client.Put(ctx, "/buckets/"+bucket+"/cors", req, nil)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(bucket)

	return nil
}

func resourceBucketCORSRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Id()

	var result map[string]interface{}
	err := client.Get(ctx, "/buckets/"+bucket+"/cors", &result)
	if err != nil {
		d.SetId("")
		return nil
	}

	d.Set("bucket", bucket)
	d.Set("rule", result["rules"])

	return nil
}

func resourceBucketCORSDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Id()

	err := client.Delete(ctx, "/buckets/"+bucket+"/cors")
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return nil
}
