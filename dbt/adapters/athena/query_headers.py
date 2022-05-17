import dbt.adapters.base.query_headers

class _QueryComment(dbt.adapters.base.query_headers._QueryComment):
    """
    Athena DDL does not always respect /* ... */ block quotations. 
    This function is the same as _QueryComment.add except that 
    a leading "-- " is prepended to the query_comment and any newlines
    in the query_comment are replaced with " ". This allows the default
    query_comment to be added to `create external table` statements.
    """
    def add(self, sql: str) -> str:
        if not self.query_comment:
            return sql

        if self.append:
            # replace last ';' with '<comment>;'
            sql = sql.rstrip()
            if sql[-1] == ";":
                sql = sql[:-1]
                return "{}\n-- /* {} */;".format(sql, self.query_comment.strip().replace("\n", " "))

            return "{}\n-- /* {} */".format(sql, self.query_comment.strip().replace("\n", " "))

        return "-- /* {} */\n{}".format(self.query_comment.strip().replace("\n", " "), sql)


dbt.adapters.base.query_headers._QueryComment = _QueryComment
