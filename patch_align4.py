path = "ckan_ingestor/ckan_dataset_fetcher.py"
with open(path) as f:
    c = f.read()

# Replace the whole _align_schema method
old_method = '''    @staticmethod
    def _align_schema(
        table: pyarrow.Table, base_schema: pyarrow.Schema
    ) -> pyarrow.Table:
        """Align a table's schema to match the base schema, casting to string on mismatch."""
        for field in base_schema:
            if field.name not in table.column_names:
                table = table.append_column(
                    field, pyarrow.nulls(table.num_rows, field.type)
                )
            elif table.schema.field(field.name).type != field.type:
                idx = table.schema.get_field_index(field.name)
                col = table.column(field.name)
                handled = False
                # 1) Try direct cast to base type
                try:
                    table = table.set_column(idx, field.name, col.cast(field.type))
                    handled = True
                except (
                    pyarrow.ArrowInvalid,
                    pyarrow.ArrowTypeError,
                    pyarrow.ArrowNotImplementedError,
                ):
                    pass
                if handled:
                    continue
                # 2) Scalar fallback: cast to string (preserves data)
                if not pyarrow.types.is_list(field.type):
                    try:
                        table = table.set_column(
                            idx, field.name, col.cast(pyarrow.string())
                        )
                        handled = True
                    except (
                        pyarrow.ArrowInvalid,
                        pyarrow.ArrowTypeError,
                        pyarrow.ArrowNotImplementedError,
                    ):
                        pass
                if handled:
                    continue
                # 3) Last resort: nulls of base type (list/struct mismatches)
                table = table.set_column(
                    idx, field.name,
                    pyarrow.nulls(table.num_rows, field.type),
                )
        # Reorder columns to match base schema
        return table.select([f.name for f in base_schema])'''

new_method = '''    @staticmethod
    def _align_schema(
        table: pyarrow.Table, base_schema: pyarrow.Schema
    ) -> pyarrow.Table:
        """Align a table's schema to match the base schema.
        Missing columns → nulls. Type mismatches → cast to string (scalars)
        or nulls of base type (lists/structs).
        """
        for field in base_schema:
            if field.name not in table.column_names:
                table = table.append_column(
                    field, pyarrow.nulls(table.num_rows, field.type)
                )
                continue

            if table.schema.field(field.name).type == field.type:
                continue

            idx = table.schema.get_field_index(field.name)
            col = table.column(field.name)

            # Try direct cast
            try:
                table = table.set_column(idx, field.name, col.cast(field.type))
                continue
            except (
                pyarrow.ArrowInvalid,
                pyarrow.ArrowTypeError,
                pyarrow.ArrowNotImplementedError,
            ):
                pass

            # Scalar: cast to string
            if not pyarrow.types.is_list(field.type):
                table = table.set_column(
                    idx, field.name, col.cast(pyarrow.string())
                )
            else:
                # List/struct mismatch: nulls of base type
                table = table.set_column(
                    idx, field.name,
                    pyarrow.nulls(table.num_rows, field.type),
                )

        return table.select([f.name for f in base_schema])'''

c = c.replace(old_method, new_method)
with open(path, "w") as f:
    f.write(c)
print("Done")
