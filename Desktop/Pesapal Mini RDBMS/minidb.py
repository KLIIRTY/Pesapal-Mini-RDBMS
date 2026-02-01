import shlex

class Table:
    def __init__(self, name, columns, primary_key=None, unique_keys=None):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.unique_keys = unique_keys or []
        self.rows = []

    def insert(self, values):
        row = dict(zip(self.columns, values))

        # Primary key enforcement
        if self.primary_key:
            for r in self.rows:
                if r[self.primary_key] == row[self.primary_key]:
                    raise Exception("Primary key violation")

        # Unique key enforcement
        for key in self.unique_keys:
            for r in self.rows:
                if r[key] == row[key]:
                    raise Exception(f"Unique key violation on {key}")

        self.rows.append(row)

    def select(self, where=None):
        if not where:
            return self.rows
        key, value = where
        return [r for r in self.rows if str(r.get(key)) == value]

    def update(self, where, updates):
        key, value = where
        for r in self.rows:
            if str(r.get(key)) == value:
                r.update(updates)

    def delete(self, where):
        key, value = where
        self.rows = [r for r in self.rows if str(r.get(key)) != value]


class MiniRDBMS:
    def __init__(self):
        self.tables = {}

    def create_table(self, name, columns, primary_key=None, unique_keys=None):
        self.tables[name] = Table(name, columns, primary_key, unique_keys)

    def get_table(self, name):
        return self.tables.get(name)
    def join(self, table1, table2, key1, key2):
        t1 = self.tables.get(table1)
        t2 = self.tables.get(table2)

        if not t1 or not t2:
            raise Exception("Table not found")

        results = []
        for r1 in t1.rows:
            for r2 in t2.rows:
                if r1[key1] == r2[key2]:
                    combined = {f"{table1}.{k}": v for k, v in r1.items()}
        combined.update({f"{table2}.{k}": v for k, v in r2.items()})
        results.append(combined)
        return results


db = MiniRDBMS()

def repl():
    print("MiniRDBMS started. Type EXIT to quit.")
    while True:
        try:
            command = input("db> ").strip()
            if command.upper() == "EXIT":
                break

            tokens = shlex.split(command)
            if not tokens:
                continue


            if tokens[0].upper() == "CREATE":
                table = tokens[2]
                cols = tokens[3].split(",")
                pk = tokens[4] if len(tokens) > 4 else None
                db.create_table(table, cols, primary_key=pk)
                print(f"Table '{table}' created.")

            elif tokens[0].upper() == "INSERT":
                table = db.get_table(tokens[2])
                values = tokens[3].split(",")
                table.insert(values)
                print("Row inserted.")

            elif tokens[0].upper() == "SELECT":
                table = db.get_table(tokens[3])
                if "WHERE" in tokens:
                    idx = tokens.index("WHERE")
                    where = (tokens[idx + 1], tokens[idx + 2])
                    rows = table.select(where)
                else:
                    rows = table.select()
                for r in rows:
                    print(r)

            elif tokens[0].upper() == "UPDATE":
                table = db.get_table(tokens[1])
                where = (tokens[3], tokens[4])
                updates = dict([tokens[6].split("=")])
                table.update(where, updates)
                print("Row updated.")

            elif tokens[0].upper() == "DELETE":
                table = db.get_table(tokens[2])
                where = (tokens[4], tokens[5])
                table.delete(where)
                print("Row deleted.")

            elif tokens[0].upper() == "JOIN":
                results = db.join(
                    tokens[1], tokens[2],
                    tokens[3], tokens[4]
                )
                for r in results:
                    print(r)

            else:
                print("Unknown command.")

        except Exception as e:
            print("Error:", e)

            

if __name__ == "__main__":
    repl()
