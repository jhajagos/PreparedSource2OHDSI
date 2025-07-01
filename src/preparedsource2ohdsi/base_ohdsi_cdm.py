class OutputClass(object):
    def fields(self):
        self.include_source_id_fields = True
        field_name = self._fields()
        return field_name

    def table_name(self):
        return self._table_name()

    def data_types(self):
        return self._data_types()


class OHDSIOutputClass(OutputClass):
    """Adds extra fields for linking back to source table"""
    def fields(self):
        field_name = self._fields()
        self.include_source_id_fields = True
        if self.include_source_id_fields:
            field_name += ["s_id", "g_source_table_name", "s_g_id"]

        return field_name

    def table_name(self):
        return self._table_name()

    def data_types(self):
        return self._data_types()

    def version(self):
        return None