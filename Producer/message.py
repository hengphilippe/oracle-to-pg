from typing import Optional
from pydantic import BaseModel
import time
from typing import Optional


class Message(BaseModel):
    scn: int
    seg_owner = 'none'
    table_name = 'none'
    timestamp: int
    sql_redo = 'none'
    operation = 'none'
    data: Optional[dict]
    before: Optional[dict]


if __name__ == "__main__":
    message = Message(scn=123, seg_owner="SOE", table_name="LOGON", sql_redo="SELECT * FROM FK",
                      operation="READ", data='{"A":"132"}', timestamp=int(time.time()))

    print(message.scn)
