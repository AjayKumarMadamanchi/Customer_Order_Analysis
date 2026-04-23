{% snapshot snapshot_hosts %}

{{
    config(
      target_database='dev',
      target_schema='snapshots',
      unique_key='host_id',
      strategy='check',
      check_cols=['is_superhost']
    )
}}

select * from {{ ref('hosts') }}

{% endsnapshot %}