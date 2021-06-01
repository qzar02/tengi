
class Table:
    def __init__(
        self,
        name,
        layer,
        bucket,
        key,
        format,
        schema=None,
        template_key=None,
        datamask=None,
        options={},
    ):
        self.name = name
        self.layer = layer
        self.uri_scheme = "file://"
        self.bucket = bucket
        self.key = key
        self.template_key = template_key
        self.schema = schema
        self.options = options
        self.datamask = datamask
        self.format = format
        self._df = None

    @property
    def id(self):
        return f"{self.layer}_{self.name}"

    @property
    def absolute_path(self):
        return f"{self.uri_scheme}/{self.bucket}/{self.key}"

    @property
    def df(self):
        return self._df

    def read(self):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        self._df = (
            spark.read.format(self.format)
            .options(**self.options)
            .load(self.absolute_path)
        )

        return self

    def add_timestamp(self):
        from pyspark.sql import functions as f
        self._df = (
            self.df.withColumn('dt', f.date_format(f.current_date(), 'yyyyMMdd'))
            .withColumn('ts', f.unix_timestamp(f.current_timestamp()))
        )

        return self

    def write(self, mode, options={}, partition_by=[]):
        self.add_timestamp()

        writer = (
            self.df.write.format(self.format)
            .options(**options)
        )

        writer = writer.partitionBy(*partition_by) if partition_by else writer

        writer.save(self.absolute_path, mode=mode)

        return self

    def init(self, df):
        self._df = df
        return self

    def encrypt(self):
        if self.datamask:
            self._df = self.datamask.encrypt(self.df)

        return self

    def decrypt(self, fields):
        if self.datamask:
            self._df = self.datamask.decrypt(self.df, fields)
        return self

    def transform(self, fn_transform):
        self._df = fn_transform(self.df, self)
        return self

    @property
    def stats(self):
        return self.df.describe()
