{% snapshot snapshot_hosts %}

{{
    config(
      target_schema='snapshots',
      unique_key='HOST_ID',
      strategy='check',
      check_cols=['IS_SUPERHOST']
    )
}}

select * from {{ ref('hosts') }}

{% endsnapshot %}