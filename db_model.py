import peewee

database = peewee.Proxy()


class BaseModel(peewee.Model):
    class Meta:
        database = database


class SmartHome(BaseModel):
    ts = peewee.DateTimeField
    device: peewee.TextField()
    value: peewee.FloatField()
    datenname: peewee.TextField()


def create_tables():
    database.create_tables([SmartHome])
