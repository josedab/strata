// Object resource for Terraform

package strata

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func resourceObject() *schema.Resource {
	return &schema.Resource{
		Description: "Manages a Strata object",

		CreateContext: resourceObjectCreate,
		ReadContext:   resourceObjectRead,
		UpdateContext: resourceObjectUpdate,
		DeleteContext: resourceObjectDelete,

		Schema: map[string]*schema.Schema{
			"bucket": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The bucket name",
			},
			"key": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The object key",
			},
			"source": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"content", "content_base64"},
				Description:   "Path to a file to upload",
			},
			"content": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"source", "content_base64"},
				Description:   "Content to upload as string",
			},
			"content_base64": {
				Type:          schema.TypeString,
				Optional:      true,
				ConflictsWith: []string{"source", "content"},
				Description:   "Content to upload as base64",
			},
			"content_type": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "application/octet-stream",
				Description: "Content type of the object",
			},
			"storage_class": {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "STANDARD",
				Description: "Storage class for the object",
			},
			"metadata": {
				Type:        schema.TypeMap,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Custom metadata",
			},
			"cache_control": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Cache-Control header",
			},
			"content_disposition": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Content-Disposition header",
			},
			"content_encoding": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Content-Encoding header",
			},
			// Computed attributes
			"etag": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "ETag of the object",
			},
			"version_id": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Version ID of the object",
			},
			"size": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Size of the object in bytes",
			},
		},
	}
}

func resourceObjectCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Get("bucket").(string)
	key := d.Get("key").(string)

	// Get content
	var content []byte
	var err error

	if v, ok := d.GetOk("source"); ok {
		content, err = os.ReadFile(v.(string))
		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to read source file: %w", err))
		}
	} else if v, ok := d.GetOk("content"); ok {
		content = []byte(v.(string))
	} else if v, ok := d.GetOk("content_base64"); ok {
		content, err = base64.StdEncoding.DecodeString(v.(string))
		if err != nil {
			return diag.FromErr(fmt.Errorf("failed to decode base64 content: %w", err))
		}
	} else {
		return diag.Errorf("one of source, content, or content_base64 must be specified")
	}

	req := map[string]interface{}{
		"content":       base64.StdEncoding.EncodeToString(content),
		"content_type":  d.Get("content_type").(string),
		"storage_class": d.Get("storage_class").(string),
	}

	if v, ok := d.GetOk("metadata"); ok {
		req["metadata"] = v.(map[string]interface{})
	}
	if v, ok := d.GetOk("cache_control"); ok {
		req["cache_control"] = v.(string)
	}
	if v, ok := d.GetOk("content_disposition"); ok {
		req["content_disposition"] = v.(string)
	}
	if v, ok := d.GetOk("content_encoding"); ok {
		req["content_encoding"] = v.(string)
	}

	var result map[string]interface{}
	err = client.Put(ctx, fmt.Sprintf("/buckets/%s/objects/%s", bucket, key), req, &result)
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to upload object: %w", err))
	}

	d.SetId(fmt.Sprintf("%s/%s", bucket, key))

	if etag, ok := result["etag"].(string); ok {
		d.Set("etag", etag)
	}
	if versionId, ok := result["version_id"].(string); ok {
		d.Set("version_id", versionId)
	}

	return resourceObjectRead(ctx, d, m)
}

func resourceObjectRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Get("bucket").(string)
	key := d.Get("key").(string)

	var obj Object
	err := client.Get(ctx, fmt.Sprintf("/buckets/%s/objects/%s/metadata", bucket, key), &obj)
	if err != nil {
		d.SetId("")
		return nil
	}

	d.Set("etag", obj.ETag)
	d.Set("size", obj.Size)
	d.Set("content_type", obj.ContentType)
	d.Set("storage_class", obj.StorageClass)

	if obj.Metadata != nil {
		d.Set("metadata", obj.Metadata)
	}

	return nil
}

func resourceObjectUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	// Objects are immutable - recreate on change
	return resourceObjectCreate(ctx, d, m)
}

func resourceObjectDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Get("bucket").(string)
	key := d.Get("key").(string)

	err := client.Delete(ctx, fmt.Sprintf("/buckets/%s/objects/%s", bucket, key))
	if err != nil {
		return diag.FromErr(fmt.Errorf("failed to delete object: %w", err))
	}

	d.SetId("")

	return nil
}

func resourceUser() *schema.Resource {
	return &schema.Resource{
		Description: "Manages a Strata user",

		CreateContext: resourceUserCreate,
		ReadContext:   resourceUserRead,
		UpdateContext: resourceUserUpdate,
		DeleteContext: resourceUserDelete,

		Schema: map[string]*schema.Schema{
			"username": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The username",
			},
			"email": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Email address",
			},
			"role": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "User role (admin, operator, viewer)",
			},
			"created_at": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Creation timestamp",
			},
		},
	}
}

func resourceUserCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	username := d.Get("username").(string)

	req := map[string]interface{}{
		"username": username,
		"role":     d.Get("role").(string),
	}

	if v, ok := d.GetOk("email"); ok {
		req["email"] = v.(string)
	}

	var user User
	err := client.Post(ctx, "/admin/users", req, &user)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(username)

	return resourceUserRead(ctx, d, m)
}

func resourceUserRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	username := d.Id()

	var user User
	err := client.Get(ctx, "/admin/users/"+username, &user)
	if err != nil {
		d.SetId("")
		return nil
	}

	d.Set("username", user.Username)
	d.Set("email", user.Email)
	d.Set("role", user.Role)
	d.Set("created_at", user.CreatedAt.Format("2006-01-02T15:04:05Z"))

	return nil
}

func resourceUserUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	username := d.Id()

	req := map[string]interface{}{}

	if d.HasChange("role") {
		req["role"] = d.Get("role").(string)
	}
	if d.HasChange("email") {
		req["email"] = d.Get("email").(string)
	}

	err := client.Put(ctx, "/admin/users/"+username, req, nil)
	if err != nil {
		return diag.FromErr(err)
	}

	return resourceUserRead(ctx, d, m)
}

func resourceUserDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	username := d.Id()

	err := client.Delete(ctx, "/admin/users/"+username)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return nil
}

func resourceAccessKey() *schema.Resource {
	return &schema.Resource{
		Description: "Manages a Strata access key",

		CreateContext: resourceAccessKeyCreate,
		ReadContext:   resourceAccessKeyRead,
		DeleteContext: resourceAccessKeyDelete,

		Schema: map[string]*schema.Schema{
			"description": {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "Description of the access key",
			},
			"access_key_id": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The access key ID",
			},
			"secret_key": {
				Type:        schema.TypeString,
				Computed:    true,
				Sensitive:   true,
				Description: "The secret key (only available at creation)",
			},
			"created_at": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Creation timestamp",
			},
		},
	}
}

func resourceAccessKeyCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	req := map[string]interface{}{}

	if v, ok := d.GetOk("description"); ok {
		req["description"] = v.(string)
	}

	var key AccessKey
	err := client.Post(ctx, "/admin/keys", req, &key)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(key.AccessKeyID)
	d.Set("access_key_id", key.AccessKeyID)
	d.Set("secret_key", key.SecretKey)
	d.Set("created_at", key.CreatedAt.Format("2006-01-02T15:04:05Z"))

	return nil
}

func resourceAccessKeyRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	keyId := d.Id()

	var key AccessKey
	err := client.Get(ctx, "/admin/keys/"+keyId, &key)
	if err != nil {
		d.SetId("")
		return nil
	}

	d.Set("access_key_id", key.AccessKeyID)
	d.Set("description", key.Description)
	d.Set("created_at", key.CreatedAt.Format("2006-01-02T15:04:05Z"))
	// Note: secret_key is only available at creation

	return nil
}

func resourceAccessKeyDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	keyId := d.Id()

	err := client.Delete(ctx, "/admin/keys/"+keyId)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("")

	return nil
}
