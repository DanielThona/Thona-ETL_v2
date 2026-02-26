create schema if not exists logs;

create table if not exists logs.etl_log_cargas (
  id_log            bigint identity(1,1),
  run_id            varchar(64) not null,
  flow_name         varchar(128) not null,
  origen            varchar(32) not null,  -- ORACLE | API | MANUAL
  modo              varchar(32) not null,  -- full | incremental | manual
  tabla_destino     varchar(256) not null,

  fecha_inicio      timestamp not null,
  fecha_fin         timestamp null,
  duracion_seg      integer null,

  registros_extraidos integer default 0,
  registros_ok        integer default 0,
  registros_fallidos  integer default 0,

  status            varchar(16) not null, -- RUNNING | SUCCESS | FAILED
  error_resumen      varchar(2000) null,
  archivo_errores    varchar(512) null,

  usuario            varchar(128) null,

  created_at        timestamp default getdate()
)
diststyle auto;

-- recomendable para consultas por tabla/tiempo
-- (en Redshift sortkey se define al crear; aquí lo dejamos simple en MVP)