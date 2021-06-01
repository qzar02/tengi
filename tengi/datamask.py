class DataMask:
    def __init__(self, fields, table_reverse, table_bkp=None):
        self.fields = fields
        self.table_reverse = table_reverse
        self.table_backup = table_bkp

    def mask(self, col):
        from pyspark.sql import functions as f
        return f.sha2(f.sha2(f.col(col), 512), 256)

    def encrypt(self, df):
        from pyspark.sql import functions as f
        _df = df

        for field in self.fields:
            field_mask = f'{field["field"]}_mask'
            _df = _df.withColumn(field_mask, self.mask(field["field"]))

            df_reverse = (
                _df.select(
                    f.col(field["field"]).alias("origin"),
                    f.col(field_mask).alias("encrypt"))
                .distinct()
                .cache()
            )

            _df = (
                _df
                .withColumn(field["field"], f.col(field_mask))
                .drop(field_mask)
            )
            print(f"saving {field['domain']}")

            self.table_reverse.key = self.table_reverse.template_key.format(
                domain=field["domain"]
            )
            self.table_reverse.init(df_reverse).write(mode='append')

            if self.table_backup:
                print(f"saving {field['domain']} backup")

                self.table_backup.key = self.table_backup.template_key.format(
                    domain=field["domain"]
                )
                self.table_backup.init(df_reverse).write(mode='append')

        return _df

    def decrypt(self, df, fields):
        from pyspark.sql import functions as f
        _df = df
        for field in fields:
            self.table_reverse.key = self.table_reverse.template_key.format(
                domain=field["domain"]
            )
            df_map = (
                self.table_reverse.read().df
                .distinct()
                .select(
                    f.col("encrypt").alias(field["field"]),
                    f.col("origin").alias(f'{field["field"]}_dec'),
                )
            )

            _df = _df.join(df_map, [field["field"]], "left")

        return _df
