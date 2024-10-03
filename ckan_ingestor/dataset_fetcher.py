from abc import ABC, abstractmethod

import pyarrow
import pyarrow as pa


class DatasetFetcher(ABC):

    @abstractmethod
    def do_fetch(self) -> pyarrow.Table:
        pass

    def fetch(self) -> pyarrow.Table:
        dataset = self.do_fetch()
        return self.sanitize_schema(dataset)

    def sanitize_schema(self, dataset: pyarrow.Table) -> pyarrow.Table:
        schema = dataset.schema
        fields = [(i, schema.field(i)) for name in schema.names for i in schema.get_all_field_indices(name)]
        columns_to_drop = []
        for i, field in fields:
            if field.type.equals(pa.null()):
                columns_to_drop.append(field.name)
            if field.type.num_fields > 0:
                new_field = self.sanitize_type(field)
                if new_field is None:
                    columns_to_drop.append(field.name)
                else:
                    column = dataset[field.name]
                    new_column = column.cast(new_field.type)
                    dataset = dataset.set_column(i, new_field, new_column)
        dataset = dataset.drop_columns(columns_to_drop)
        return dataset

    def sanitize_type(self, field: pyarrow.Field) -> pyarrow.Field | None:
        if field.type.num_fields > 0:
            fields = [self.sanitize_type(field.type.field(i)) for i in range(0, field.type.num_fields)]
            fields = [field for field in fields if field is not None]
            if not fields:
                return None
            field_type = field.type
            if pyarrow.types.is_list(field_type) or pyarrow.types.is_fixed_size_list(field_type):
                new_type = pyarrow.list_(fields[0])
            elif pyarrow.types.is_large_list(field_type):
                new_type = pyarrow.large_list(fields[0])
            elif pyarrow.types.is_list_view(field_type):
                new_type = pyarrow.list_view(fields[0])
            elif pyarrow.types.is_large_list_view(field_type):
                new_type = pyarrow.large_list_view(fields[0])
            elif pyarrow.types.is_struct(field_type):
                new_type = pyarrow.struct(fields)
            elif pyarrow.types.is_run_end_encoded(field_type):
                new_type = pyarrow.run_end_encoded(run_end_type=field_type.run_end_type, value_type=fields[0].type)
            elif pyarrow.types.is_map(field_type):
                new_type = pyarrow.map_(key_type=field_type.key_type, item_type=fields[0],
                                        keys_sorted=field_type.keys_sorted)
            elif pyarrow.types.is_dictionary(field_type):
                new_type = pyarrow.dictionary(index_type=field_type.index_type, value_type=fields[0].type,
                                              bool_ordered=field_type.ordered)
            else:
                raise TypeError(f"type {field.type} from field {field.name} is not supported")
            return pyarrow.field(field.name, type=new_type, nullable=field.nullable, metadata=field.metadata)
        elif field.type.equals(pa.null()):
            return None
        else:
            return field
