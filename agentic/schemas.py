from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

SourceSystem = Literal["oozie", "pyspark2", "sqoop", "hiveql", "excel"]

SOURCE_TYPES = ("hive_table", "parquet", "orc", "csv", "json", "avro", "jdbc", "iceberg_table", "delta", "kafka")
TARGET_TYPES = ("hive_table", "iceberg_table", "parquet", "orc", "csv", "jdbc", "delta")
TRANSFORM_TYPES = (
    "select", "filter", "derive_columns", "nested_derive", "filter_dedupe_flatten", "join", "join_derive",
    "window", "aggregate", "explode_aggregate", "build_array_of_struct", "collect_to_array_of_struct",
    "collect_to_map", "union", "spark_sql", "custom",
)


# ---------------------------------------------------------------------------------------
# Legacy flat model (Oozie / Sqoop / Excel style): a list of steps
# ---------------------------------------------------------------------------------------
class StepDetails(BaseModel):
    column: Optional[str] = None
    target_type: Optional[str] = None
    date_format: Optional[str] = None
    condition: Optional[str] = None


class PipelineStep(BaseModel):
    step_id: int
    name: str
    type: Literal["source", "transformation", "sink"]
    action: str  # e.g. read_parquet, cast_column, sqoop_import, spark_sql, write_table
    path: Optional[str] = None
    source: Optional[str] = None  # data lineage: upstream step name
    depends_on: List[str] = Field(default_factory=list)  # orchestration-only deps
    details: Optional[StepDetails] = None
    params: Dict[str, str] = Field(default_factory=dict)  # source-specific settings


# ---------------------------------------------------------------------------------------
# Rich model: typed sources -> transformations -> targets, with lineage
# ---------------------------------------------------------------------------------------
class SchemaField(BaseModel):
    """One column. `type` is a Spark type string ("DECIMAL(18,2)", "ARRAY<STRING>",
    "STRUCT<a:INT>") or a bare complex kind (STRUCT / ARRAY / MAP) described by the
    structured keys below."""
    name: str
    type: str
    fields: Optional[List["SchemaField"]] = None  # STRUCT members, or members of ARRAY element
    element_type: Optional[str] = None            # ARRAY
    key_type: Optional[str] = None                # MAP
    value_type: Optional[str] = None              # MAP
    known_keys: Optional[List[str]] = None        # MAP: keys the job actually reads
    rename_to: Optional[str] = None


class Source(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)  # e.g. format options, jdbc settings
    id: str
    type: str
    object: Optional[str] = None       # db.table for table sources
    path: Optional[str] = None         # location for file sources
    pre_filter: Optional[str] = None   # may reference :params
    schema_: List[SchemaField] = Field(default_factory=list, alias="schema")


class Transformation(BaseModel):
    """Generic node. The common keys are typed; anything type-specific (rules, explode,
    steps, element_schema, ...) is kept as extra fields so no logic is lost."""
    model_config = ConfigDict(extra="allow")
    id: str
    type: str
    inputs: List[str] = Field(default_factory=list)
    columns: Optional[List[Dict[str, Any]]] = None       # {name, expr, op?}
    select: Optional[List[str]] = None
    filter: Optional[str] = None
    dedupe_keys: Optional[List[str]] = None
    joins: Optional[List[Dict[str, Any]]] = None         # {left, right, type, on, hint?}
    window: Optional[Dict[str, Any]] = None              # {partition_by, order_by, frame}
    group_by: Optional[List[str]] = None
    aggregations: Optional[List[Dict[str, Any]]] = None  # {name, expr}
    sql: Optional[str] = None                            # spark_sql / custom escape hatch
    notes: Optional[str] = None                          # migration notes / caveats


class Target(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)
    id: str
    input: str
    type: str
    object: Optional[str] = None
    path: Optional[str] = None
    format: Optional[str] = None
    mode: Optional[str] = None              # append | overwrite | overwrite_dynamic_partition | merge
    method: Optional[str] = None            # insertInto | saveAsTable | save | merge
    partition_by: List[str] = Field(default_factory=list)
    schema_: List[SchemaField] = Field(default_factory=list, alias="schema")
    properties: Dict[str, str] = Field(default_factory=dict)  # e.g. Iceberg table properties


class LineageEntry(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    target_column: str
    from_: List[str] = Field(default_factory=list, alias="from")
    via: List[str] = Field(default_factory=list)  # transformation ids the value flows through


class JobInfo(BaseModel):
    model_config = ConfigDict(extra="allow")
    name: str
    domain: Optional[str] = None
    use_case: Optional[str] = None
    engine: Optional[str] = None
    engine_version: Optional[str] = None
    source_file: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    spark_conf: Dict[str, Any] = Field(default_factory=dict)


class PipelineBlueprint(BaseModel):
    """Common YAML contract every ingestion agent converges on.

    Two shapes, either or both:
      * flat   : `steps` (orchestration-style jobs: Oozie, Sqoop, Excel maps)
      * rich   : `job` + `sources` + `transformations` + `targets` + `lineage`
                 (data-logic-heavy jobs: nested types, windows, higher-order functions)
    """
    model_config = ConfigDict(populate_by_name=True)
    name: Optional[str] = None  # defaults to job.name
    source_system: Optional[SourceSystem] = None
    target_engine: Literal["pyspark_3.x", "pyspark_2.x"] = "pyspark_3.x"
    schedule: Optional[str] = None  # cron or Airflow preset, e.g. "0 2 * * *" / "@daily"
    global_configs: dict = Field(default_factory=dict)
    steps: List[PipelineStep] = Field(default_factory=list)

    job: Optional[JobInfo] = None
    sources: List[Source] = Field(default_factory=list)
    transformations: List[Transformation] = Field(default_factory=list)
    targets: List[Target] = Field(default_factory=list)
    lineage: List[LineageEntry] = Field(default_factory=list)

    def model_post_init(self, _ctx) -> None:
        if not self.name and self.job:
            self.name = self.job.name

    def to_dict(self) -> Dict[str, Any]:
        """YAML-ready dict using the public key names (schema/from), without empty values."""
        d = self.model_dump(exclude_none=True, by_alias=True)
        return {k: v for k, v in d.items() if v not in ([], {})}

    @property
    def is_rich(self) -> bool:
        return bool(self.transformations or self.sources or self.targets)

    @property
    def parameters(self) -> Dict[str, Any]:
        return self.job.parameters if self.job else {}

    def node_names(self) -> List[str]:
        """Names every generated PySpark script must cover (df_<name> variables)."""
        names = [s.name for s in self.steps]
        names += [s.id for s in self.sources] + [t.id for t in self.transformations]
        names += [t.id for t in self.targets]
        return names

    def task_names(self) -> List[str]:
        """Airflow task_ids required: one per flat step; a rich job is one Spark task
        (task_id == blueprint name) unless it also has flat steps."""
        names = [s.name for s in self.steps]
        if self.is_rich:
            names.append(self.name or "job")
        return names


SchemaField.model_rebuild()


