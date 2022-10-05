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


    /**
     * basic - Id, FirstName, LastName
     * donor_info - New donation fields.
     * contact_general - A lot of fields that are transferred to FileMaker.
     */
	public function transfer($sobject = "Contact", $list = "basic") {

		$date = new DateTime();
		$date->modify('-120 day');
		$date = $date->format('Y-m-d');


		$api = loadApi();

        $key = strtolower($sobject);

		$affectedRows = 0;

		$fields = array(
			"IsDeleted",
			"OcdlaIsPaperless__c",
			"Ocdla_Home_Address_Publish__c",
			"Ocdla_Publish_Work_Phone__c",
			"Ocdla_Noflag__c",
			"Ocdla_Is_Board_Member__c",
			"Ocdla_Is_Board_Member_Past__c",
			"Ocdla_Is_Board_Past_President__c",
			"Ocdla_Current_Member_Flag__c"
		);


		$fields = config($list);
		$fields []= "CreatedDate";
		$fields []= "LastModifiedDate";
		$fieldListq = implode(", ", $fields);

		// Format of Select query to Salesforce.
		$select = "SELECT %s FROM Contact WHERE LastModifiedDate >= %sT00:00:00Z";

		// Format of Insert query to MySQL.
		$insert = "INSERT INTO force_contact (%s) VALUES %s AS new(%s) ON DUPLICATE KEY UPDATE %s";

		// Prepare the MySQL insert query.
		$updates = array_filter($fields, function($f) { return $f != "Id"; });
		$updates = array_map(function($f) { return "{$f} = new.{$f}";}, $updates);
		$fieldList = implode(", ", $fields);
		$updateList = implode(", ", $updates);

		
		// Get records from Salesforce.
		$query = sprintf($select, $fieldListq, $date);


		$result = $this->mysqli->query("DESCRIBE force_contact");

		$schema = [];

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


		// Set to non-false value when paging.
		$next = false;

		$out = [];

        do {

			// We will insert/update rows into the appropriate table.
			$rows = array();

			$out []= $next ? "Starting batch {$next}." : "Performing query $query.";
			$resp = $next ? $api->queryUrl($next) : $api->queryIncludeDeleted($query, true);

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
			$query = sprintf($insert, $fieldList, $rowList, $fieldList, $updateList);
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






}