<?php




use Http\HttpResponse;
use Http\HttpHeader;
use Http\Http;
use Http\HttpRequest;
use Mysql\Database;



class SalesforceModule extends Module {

    public function __construct() {

        parent::__construct();
    }


    /**
     * basic - Id, FirstName, LastName
     * donor_info - New donation fields.
     * contact_general - A lot of fields that are transferred to FileMaker.
     */
	public function transfer($sobject = "Contact", $list = "basic", $date = "2020-01-01") {

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

		// Select query to Salesforce.
		$select = "SELECT %s FROM Contact WHERE LastModifiedDate >= %sT00:00:00Z";

		// Insert query to MySQL.
		$insert = "INSERT INTO force_contact (%s) VALUES %s AS new(%s) ON DUPLICATE KEY UPDATE %s";

		// Prepare the MySQL insert query.
		$updates = array_filter($fields, function($f) { return $f != "Id"; });
		$updates = array_map(function($f) { return "{$f} = new.{$f}";}, $updates);
		$fieldList = implode(", ", $fields);
		$updateList = implode(", ", $updates);

		
		// Get records from Salesforce.
		$query = sprintf($select, $fieldListq, $date);


		$params = array(
			"host" => config("salesforce.transfer.host"),
			"user" => config("salesforce.transfer.user"),
			"password" => config("salesforce.transfer.password"),
			"name" => config("salesforce.transfer.database")
		);





		Database::setDefault($params);
		$db = new Database();
		$conn = $db->getConnection();

		$conn->set_charset("utf8mb4");
		$conn->query("SET NAMES utf8mb4 COLLATE utf8mb4_0900_as_cs");
		$conn->query("LOCK TABLES force_contact WRITE");


		// Set to non-false value when paging.
		$next = false;

		$out = [];

        do {

			$out []= "Starting batch {$next}.";

			// We will insert/update rows into the appropriate table.
			$rows = array();

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
					if(in_array($field, array("CreatedDate","LastModifiedDate"))) {
						$value = explode("+",$value)[0];
					}
					$row []= $conn->real_escape_string($value);
				}
				
				
				$row = array_map(function($v) { return "'{$v}'"; }, $row);
				$rows []= $row;
			}

			// var_dump($rows);exit;
			
			$rows = array_map(function($row) { return "(" . implode(", ",$row) . ")";}, $rows);
			$rowList = implode(", ", $rows);
			$query = sprintf($insert, $fieldList, $rowList, $fieldList, $updateList);

			// print $query;
			// exit;

			$result = $conn->query($query);
			// var_dump($conn->affected_rows);
			// var_dump($result);

			$affectedRows += $conn->affected_rows;
		
        
        } while($next);
		

		$conn->query("UNLOCK TABLES");
		$out []= ("<br />" . $affectedRows ." rows were updated.");

		return implode("<br />", $out);
	}	






}