<?php


use Http\HttpResponse;
use Http\HttpHeader;
use Http\Http;
use Http\HttpRequest;
use Mysql\Database;



class SalesforceModule extends Module {

    private $mysqli;



    public function __construct() {


		$params = array(
			"host" => config("salesforce.transfer.host"),
			"user" => config("salesforce.transfer.user"),
			"password" => config("salesforce.transfer.password"),
			"name" => config("salesforce.transfer.database")
		);


		Database::setDefault($params);
		$db = new Database();
		$mysqli = $db->getConnection();

		$mysqli->set_charset("utf8mb4");
		$mysqli->query("SET NAMES utf8mb4 COLLATE utf8mb4_0900_as_cs");
        $this->mysqli = $mysqli;


        parent::__construct();
    }





	public function transfer($sobject, $soql, $fields) {


		// asdfjkl
		$lock = "LOCK TABLES force_%s WRITE";

		// Connect to Salesforce.
		$api = loadApi();

		// Store how many updates take place after the transfer to MySQL.
		$affectedRows = 0;

		// MySQL schema needed to properly formate the MySQL query.
		$schema = null;

		// Set to non-false value when paging through Salesforce records.
		$next = false;

		// Output to display.
		$out = [];



		$lockQuery = sprintf($lock, $sobject);
		
		$schema = $this->getSchema($sobject);



		$this->mysqli->query($lockQuery);


        do {

			$out []= $next ? "Starting batch {$next}." : "Performing query $soql.";
			$resp = $next ? $api->queryUrl($next) : $api->queryIncludeDeleted($soql, true);

			if(!$resp->isSuccess()) throw new Exception($resp->getErrorMessage());


            $body = $resp->getBody();
			$totalSize = $body["totalSize"];
			$done = $body["done"];
			$records = $body["records"];
            $next = !$done ? $body["nextRecordsUrl"] : null;

			if(!count($records)) {
				return "No records founds for importing...";
			}
			
			
			// Convert objects returned from REST API to rows 
			// that can be inserted into MySQL database.
			$rows = $this->getRows($records, $fields, $schema);

			$query = $this->getInsertQuery($sobject, $rows, $fields);
			
			$out []= $query;
			// print $query;
			// exit;

			$result = $this->mysqli->query($query);
			// var_dump($conn->affected_rows);
			// var_dump($result);

			$affectedRows += $this->mysqli->affected_rows;
		
        
        } while($next);
		

		$this->mysqli->query("UNLOCK TABLES");
		$out []= ("<br />" . $affectedRows ." rows were updated.");

		return implode("<br />", $out);
	}





	public function getInsertQuery($table, $rows, $fields) {

		// Format of Insert query to MySQL.
		$insert = "INSERT INTO force_%s (%s) VALUES %s AS new(%s) ON DUPLICATE KEY UPDATE %s";

		// Prepare the MySQL insert query.
		$updates = array_filter($fields, function($f) { return $f != "Id"; });
		$updates = array_map(function($f) { return "{$f} = new.{$f}";}, $updates);
		$fieldList = implode(", ", $fields);
		$updateList = implode(", ", $updates);


		$rows = array_map(function($row) { return "(" . implode(", ",$row) . ")";}, $rows);
		$rowList = implode(", ", $rows);
			
		return sprintf($insert, strtolower($table), $fieldList, $rowList, $fieldList, $updateList);
	}




	public function getRows($records, $fields, $schema) {

		// We will insert/update rows into the appropriate table.
		$rows = array();

		
		// Assign values to the MySQL insert query.
		for($count = 0; $count < count($records); $count++) {

			$record = $records[$count];
			$row = array();

			foreach($fields as $field) {
				$value = $record[$field];

				// Convert boolean values to either 0 or 1.
				if($schema[$field]["is_int"] && is_bool($value)) {
					$value = $value ? 1 : 0;
				}
				if($schema[$field]["is_date"]) {
					$value = explode("+",$value)[0];
					$value = empty($value) ? null : $value;
				}
				$row[$field] = $schema[$field]["is_int"] ? $value : $this->mysqli->real_escape_string($value);
			}
			
			// Format string and date/time values with single quotes. Numbers shouldn't get quotes.
			$row = array_map(function($v,$k) use($schema) { 

				if(null == $v) {
					return "NULL";
				}
				else return $schema[$k]["is_int"] ? $v : "'{$v}'";
			}, $row, array_keys($row));

			$rows []= $row;
		}

		// var_dump($rows);exit;
		
		return $rows;
	}







	/**
	 * Return an array of schema statements for a given table.
	 * Schema statements can let us format the query,
	 * for example by identifying string columns that should be wrapped
	 * in quotes.
	 */
	public function getSchema($table) {

		// Get field metadata.
		$describe = "DESCRIBE force_%s";


		$query = sprintf($describe, $table);
	

		$schema = [];


		$result = $this->mysqli->query($query);

		while($info = $result->fetch_assoc()) {
			$field = $info["Field"];
			$type = $info["Type"];


			$schema[$field] = array(
				"type" => $info["Type"],
				"is_int" => (strpos($type,"int") !== false),
				"is_date" => (strpos($type,"date") !== false)
			);
		}


		return $schema;
	}





    /**
     * basic - Id, FirstName, LastName
     * donor_info - New donation fields.
     * contact_general - A lot of fields that are transferred to FileMaker.
     */
	public function transferRecentSObjectRecords($sobject = "contact", $list = "basic") {

		$date = new DateTime();
		$date->modify('-7 day');
		$date = $date->format('Y-m-d');

        $key = strtolower($sobject);

		$config = config($list);
		$fields = $config["fields"];
		$fields []= "CreatedDate";
		$fields []= "LastModifiedDate";
		$fieldListq = implode(", ", $fields);

		// Format of Select query to Salesforce.
		$select = $config["select"] ." AND LastModifiedDate >= %sT00:00:00Z";

		// Get records from Salesforce.
		$soql = sprintf($select, $fieldListq, ucwords($sobject), $date);


		return $this->transfer($sobject, $soql, $fields);
	}	




	/**
	 * Transfer 
	 */
	public function transferAll($sobject = "contact", $list = "basic") {

		$date = new DateTime();
		$date->modify('-120 day');
		$date = $date->format('Y-m-d');

        $key = strtolower($sobject);

		$config = config($list);
		$fields = $config["fields"];
		$fields []= "CreatedDate";
		$fields []= "LastModifiedDate";
		$fieldListq = implode(", ", $fields);

		// Format of Select query to Salesforce.
		$select = $config["select"];


		// Get records from Salesforce.
		$soql = sprintf($select, $fieldListq, ucwords($sobject), $date);


		return $this->transfer($sobject, $soql, $fields);
	}	






}