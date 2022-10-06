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


		// Format of Insert query to MySQL.
		$insert = "INSERT INTO force_%s (%s) VALUES %s AS new(%s) ON DUPLICATE KEY UPDATE %s";

		// Connect to Salesforce.
		$api = loadApi();

		// Store how many updates take place after the transfer to MySQL.
		$affectedRows = 0;

		// MySQL schema needed to properly formate the MySQL query.
		$schema = [];

		// Set to non-false value when paging through Salesforce records.
		$next = false;

		// Output to display.
		$out = [];



		// Prepare the MySQL insert query.
		$updates = array_filter($fields, function($f) { return $f != "Id"; });
		$updates = array_map(function($f) { return "{$f} = new.{$f}";}, $updates);
		$fieldList = implode(", ", $fields);
		$updateList = implode(", ", $updates);


		$result = $this->mysqli->query("DESCRIBE force_contact");

		

		while($info = $result->fetch_assoc()) {
			$field = $info["Field"];
			$type = $info["Type"];


			$schema[$field] = array(
				"type" => $info["Type"],
				"is_int" => (strpos($type,"int") !== false),
				"is_date" => (strpos($type,"date") !== false)
			);
		}

		// var_dump($schema);
		// exit;
		

		$this->mysqli->query("LOCK TABLES force_contact WRITE");




        do {

			// We will insert/update rows into the appropriate table.
			$rows = array();

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

				// var_dump($row);exit;
			}

			// var_dump($rows);exit;
			
			$rows = array_map(function($row) { return "(" . implode(", ",$row) . ")";}, $rows);
			$rowList = implode(", ", $rows);
			$query = sprintf($insert, strtolower($sobject), $fieldList, $rowList, $fieldList, $updateList);
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

		$fields = config($list);
		$fields []= "CreatedDate";
		$fields []= "LastModifiedDate";
		$fieldListq = implode(", ", $fields);

		// Format of Select query to Salesforce.
		$select = "SELECT %s FROM %s WHERE (NOT Email LIKE '%%qq.com%%') AND LastModifiedDate >= %sT00:00:00Z";


		// Get records from Salesforce.
		$soql = sprintf($select, $fieldListq, ucwords($sobject), $date);


		return $this->transfer($sobject, $soql, $fields);
	}	


	public function transferAll($sobject = "contact", $list = "basic") {

		$date = new DateTime();
		$date->modify('-120 day');
		$date = $date->format('Y-m-d');

        $key = strtolower($sobject);

		$fields = config($list);
		$fields []= "CreatedDate";
		$fields []= "LastModifiedDate";
		$fieldListq = implode(", ", $fields);

		// Format of Select query to Salesforce.
		$select = "SELECT %s FROM %s WHERE (NOT Email LIKE '%%qq.com%%')";


		// Get records from Salesforce.
		$soql = sprintf($select, $fieldListq, ucwords($sobject), $date);


		return $this->transfer($sobject, $soql, $fields);
	}	






}