from sqlalchemy.sql import func, select, text

def person_id_provider(db_connection):
    random_row = db_connection.execute(text("select person_id from person where person_id not in (select person_id from death)")).first()
    return getattr(random_row, "person_id", None)
