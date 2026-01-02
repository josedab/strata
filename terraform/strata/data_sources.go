// Data sources for Terraform

package strata

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func dataSourceBucket() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about a Strata bucket",

		ReadContext: dataSourceBucketRead,

		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The bucket name",
			},
			"region": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "The bucket region",
			},
			"versioning": {
				Type:        schema.TypeBool,
				Computed:    true,
				Description: "Whether versioning is enabled",
			},
			"encryption": {
				Type:        schema.TypeBool,
				Computed:    true,
				Description: "Whether encryption is enabled",
			},
			"created_at": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Creation timestamp",
			},
			"object_count": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Number of objects",
			},
			"total_size": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Total size in bytes",
			},
			"tags": {
				Type:        schema.TypeMap,
				Computed:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Bucket tags",
			},
		},
	}
}

func dataSourceBucketRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	name := d.Get("name").(string)

	var bucket Bucket
	err := client.Get(ctx, "/buckets/"+name, &bucket)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(name)
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

func dataSourceBuckets() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves a list of all buckets",

		ReadContext: dataSourceBucketsRead,

		Schema: map[string]*schema.Schema{
			"filter": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "Filter criteria",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:        schema.TypeString,
							Required:    true,
							Description: "Filter attribute name",
						},
						"values": {
							Type:        schema.TypeList,
							Required:    true,
							Elem:        &schema.Schema{Type: schema.TypeString},
							Description: "Filter values",
						},
					},
				},
			},
			"buckets": {
				Type:        schema.TypeList,
				Computed:    true,
				Description: "List of buckets",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:        schema.TypeString,
							Computed:    true,
							Description: "Bucket name",
						},
						"region": {
							Type:        schema.TypeString,
							Computed:    true,
							Description: "Bucket region",
						},
						"versioning": {
							Type:        schema.TypeBool,
							Computed:    true,
							Description: "Versioning status",
						},
						"encryption": {
							Type:        schema.TypeBool,
							Computed:    true,
							Description: "Encryption status",
						},
						"object_count": {
							Type:        schema.TypeInt,
							Computed:    true,
							Description: "Number of objects",
						},
						"total_size": {
							Type:        schema.TypeInt,
							Computed:    true,
							Description: "Total size in bytes",
						},
					},
				},
			},
		},
	}
}

func dataSourceBucketsRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	var buckets []Bucket
	err := client.Get(ctx, "/buckets", &buckets)
	if err != nil {
		return diag.FromErr(err)
	}

	// Apply filters if specified
	filters := d.Get("filter").([]interface{})
	filteredBuckets := filterBuckets(buckets, filters)

	bucketList := make([]map[string]interface{}, len(filteredBuckets))
	for i, b := range filteredBuckets {
		bucketList[i] = map[string]interface{}{
			"name":         b.Name,
			"region":       b.Region,
			"versioning":   b.Versioning,
			"encryption":   b.Encryption,
			"object_count": b.ObjectCount,
			"total_size":   b.TotalSize,
		}
	}

	d.SetId("buckets")
	d.Set("buckets", bucketList)

	return nil
}

func filterBuckets(buckets []Bucket, filters []interface{}) []Bucket {
	if len(filters) == 0 {
		return buckets
	}

	var result []Bucket
	for _, b := range buckets {
		matches := true
		for _, f := range filters {
			filter := f.(map[string]interface{})
			name := filter["name"].(string)
			values := filter["values"].([]interface{})

			var fieldValue string
			switch name {
			case "name":
				fieldValue = b.Name
			case "region":
				fieldValue = b.Region
			default:
				continue
			}

			found := false
			for _, v := range values {
				if fieldValue == v.(string) {
					found = true
					break
				}
			}
			if !found {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, b)
		}
	}

	return result
}

func dataSourceObject() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about a Strata object",

		ReadContext: dataSourceObjectRead,

		Schema: map[string]*schema.Schema{
			"bucket": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The bucket name",
			},
			"key": {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The object key",
			},
			"version_id": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Specific version to retrieve",
			},
			"etag": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "ETag of the object",
			},
			"size": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Size in bytes",
			},
			"content_type": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Content type",
			},
			"storage_class": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Storage class",
			},
			"last_modified": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Last modification timestamp",
			},
			"metadata": {
				Type:        schema.TypeMap,
				Computed:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Object metadata",
			},
		},
	}
}

func dataSourceObjectRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	bucket := d.Get("bucket").(string)
	key := d.Get("key").(string)

	path := fmt.Sprintf("/buckets/%s/objects/%s/metadata", bucket, key)
	if versionId, ok := d.GetOk("version_id"); ok {
		path += "?versionId=" + versionId.(string)
	}

	var obj Object
	err := client.Get(ctx, path, &obj)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(fmt.Sprintf("%s/%s", bucket, key))
	d.Set("etag", obj.ETag)
	d.Set("size", obj.Size)
	d.Set("content_type", obj.ContentType)
	d.Set("storage_class", obj.StorageClass)
	d.Set("last_modified", obj.LastModified.Format("2006-01-02T15:04:05Z"))

	if obj.Metadata != nil {
		d.Set("metadata", obj.Metadata)
	}

	return nil
}

func dataSourceCluster() *schema.Resource {
	return &schema.Resource{
		Description: "Retrieves information about the Strata cluster",

		ReadContext: dataSourceClusterRead,

		Schema: map[string]*schema.Schema{
			"status": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Cluster health status",
			},
			"nodes": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Total number of nodes",
			},
			"healthy_nodes": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Number of healthy nodes",
			},
			"version": {
				Type:        schema.TypeString,
				Computed:    true,
				Description: "Cluster version",
			},
			"total_capacity": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Total storage capacity in bytes",
			},
			"used_capacity": {
				Type:        schema.TypeInt,
				Computed:    true,
				Description: "Used storage capacity in bytes",
			},
			"capacity_percent": {
				Type:        schema.TypeFloat,
				Computed:    true,
				Description: "Percentage of capacity used",
			},
		},
	}
}

func dataSourceClusterRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	client := m.(*Client)

	var cluster ClusterInfo
	err := client.Get(ctx, "/cluster/health", &cluster)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId("cluster")
	d.Set("status", cluster.Status)
	d.Set("nodes", cluster.Nodes)
	d.Set("healthy_nodes", cluster.HealthyNodes)
	d.Set("version", cluster.Version)
	d.Set("total_capacity", cluster.TotalCapacity)
	d.Set("used_capacity", cluster.UsedCapacity)

	if cluster.TotalCapacity > 0 {
		percent := float64(cluster.UsedCapacity) / float64(cluster.TotalCapacity) * 100
		d.Set("capacity_percent", percent)
	}

	return nil
}
