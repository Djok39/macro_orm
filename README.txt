##ORM for PostgresSQL via macro
Main goal - build queryes at compile time.
The code is not tested well. Use in on your own risk.

you may enable verbose debugging output, by add these flags at compile time, e.g. crystal run src/app.cr -Dtxdebug
* -Dsqldebug 
	prints raw SQL on each query
* -Dspeeddebug
	show query timing on each query
* -Dtxdebug
	for transactions debugging

##usage
require "./macro_orm"

class Coin < MacroOrm
  map({
    id: Int32,
    tag: String,
    name: String,
  })
end

p Coin.all("ORDER BY id")
coin_id = Orm.db.scalar "UPDATE coin SET updated_at=CURRENT_TIMESTAMP WHERE tag=$1 RETURNING id", "BTC123"
Orm.db.exec "DELETE FROM coin_algo WHERE coin_id=$1", coin_id