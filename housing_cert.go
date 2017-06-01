/*
	Copyright IBM Corp. 2016 All Rights Reserved.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

			 http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

/*
  @Author:
    XIN LIU
  @Description:
    This is the chaincode on the housing project, including 3 data structures:
    Tenancy Contract Table:
    Insurance Contract Table;
    Request Table;
  @History:
  |-------------------------------------------------------------------------|
  |    Date    |      Owner    |            Comments                        |
  |-------------------------------------------------------------------------|
  | 05/15/2017 |      SEAN     |            Creation                        |
  |-------------------------------------------------------------------------|

*/

package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/util"
	"github.com/op/go-logging"
)

var myLogger = logging.MustGetLogger("housing")

//============================================================================
// Insurance Package Types:
//   Two different packages defined with different premium and items
//============================================================================
const InsurancePkg_Standard = 0
const InsurancePkg_Standard_Plus = 1

// TCTableName - Tenancy Contract Tabel Name
const TCTableName = "Tenancy_Contract"

// ICTableName - Insurance Contract Table Name
const ICTableName = "Insurance_Contract"

// RTableName - Request Table Name
const RTableName = "Request"

// SimpleChaincode example simple Chaincode implementation
type SimpleChaincode struct {
}

// RBACMetadata - Role Based Access Control structure
type RBACMetadata struct {
	Cert  []byte
	Sigma []byte
}

//============================================================================
// TenancyContract - A table structure to save tenancy contract
//                   between Landlord and Tenancy
//============================================================================
type TenancyContract struct {
	TenancyContractID string `json:"TenancyContractID"`
	LandlordID        string `json:"LandlordID"`
	TenantID          string `json:"TenantID"`
	Address           string `json:"Address"`
}

//==============================================================================================================================
// Init Function of a Chaincode
//==============================================================================================================================
func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	if len(args) != 0 {
		return nil, errors.New("incorrect number of arguments. Expecting 0")
	}

	return nil, nil
}

//==============================================================================================================================
// Invoke Function of a Chaincode
//                  - CreateTableTenancyContract
//					- CreateTableInsuranceContract
//					- CreateTableRequest
//                  - InsertRowRequest
//                  - DeleteRowRequest
//					- UpdateRowRequest
//					- ...
//==============================================================================================================================
func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	if function == "createTableTenancyContract" {
		table1Args := []string{TCTableName}
		return t.createTable(stub, table1Args)
	} else if function == "deleteTable" {
		return t.deleteTable(stub, args)
	} else if function == "insertRowTenancyContract" {
		return t.insertRowTenancyContract(stub, args)
	} else if function == "deleteRowTenancyContract" {
		return t.deleteRowTenancyContract(stub, args)
	} else if function == "checkCert" {
		return t.checkCert(stub, args)
	}

	return nil, errors.New("Unsupported Invoke Functions [" + function + "]")
}

//==============================================================================================================================
// Query Function of a Chaincode
//==============================================================================================================================
func (t *SimpleChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {

	if function == "getTenancyContract" {
		return t.getTenancyContract(stub, args)

	} else if function == "getTenancyContractsByID" {
		return t.getTenancyContractsByID(stub, args)

	} else if function == "getAllTenancyContracts" {
		return t.getAllTenancyContracts(stub, args)

	}

	return nil, errors.New("Received unsupported query parameter [" + function + "]")
}

//==============================================================================================================================
// createTable - Create a table
//    input - a table name (Tenancy_Contract, Insurance_Contract, Request)
//==============================================================================================================================
func (t *SimpleChaincode) createTable(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) != 1 {
		return nil, errors.New("incorrect number of auguments, Expencting 1, the name of the table")
	}

	tableName := args[0]

	fmt.Println("Start to create table " + tableName + " ...")

	var columnDefs []*shim.ColumnDefinition

	if tableName == TCTableName {

		columnDef1 := shim.ColumnDefinition{Name: "TenancyContractID", Type: shim.ColumnDefinition_STRING, Key: true}
		columnDef2 := shim.ColumnDefinition{Name: "LandlordID", Type: shim.ColumnDefinition_STRING, Key: false}
		columnDef3 := shim.ColumnDefinition{Name: "TenantID", Type: shim.ColumnDefinition_STRING, Key: false}
		columnDef4 := shim.ColumnDefinition{Name: "Address", Type: shim.ColumnDefinition_STRING, Key: false}

		columnDefs = append(columnDefs, &columnDef1)
		columnDefs = append(columnDefs, &columnDef2)
		columnDefs = append(columnDefs, &columnDef3)
		columnDefs = append(columnDefs, &columnDef4)

	} else {
		return nil, errors.New("Unsupported table name " + tableName + "!!!")
	}

	err := stub.CreateTable(tableName, columnDefs)

	if err != nil {
		return nil, fmt.Errorf("Create a table operation failed. %s", err)
	}

	fmt.Println("End to create table " + tableName + " ...")
	return nil, nil
}

//==============================================================================================================================
// deleteTable - Delete a table
//    input - a table name (Tenancy_Contract, Insurance_Contract, Request)
//==============================================================================================================================
func (t *SimpleChaincode) deleteTable(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) != 1 {
		return nil, errors.New("Incorrect number of auguments, Expencting 1, the name of the table")
	}

	tableName := args[0]

	err := stub.DeleteTable(tableName)

	if err != nil {
		return nil, fmt.Errorf("Delete a table operation failed. %s", err)
	}

	return nil, nil
}

//==============================================================================================================================
// insertRowTenancyContract - Insert a row into the table Tenancy_Contract
//    input - array of key:Value
//            [
//            "LandlordID" : "xxx";
//            "TenantID"   : "yyy";
//            "Address"    : "zzz zzz zzz"
//            ]
//==============================================================================================================================
func (t *SimpleChaincode) insertRowTenancyContract(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) != 6 {
		return nil, errors.New("Incorrect number of arguments. Expecting 6")
	}

	var landlordID string
	var tenantID string
	var address string

	for i := 0; i < len(args); i = i + 2 {
		switch args[i] {

		case "LandlordID":
			landlordID = args[i+1]
		case "TenantID":
			tenantID = args[i+1]
		case "Address":
			address = args[i+1]

		default:
			return nil, errors.New("Unsupported Parameter " + args[i] + "!!!")
		}
	}

	tenancyContractID := util.GenerateUUID()

	var columns []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: tenancyContractID}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: landlordID}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: tenantID}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: address}}
	columns = append(columns, &col1)
	columns = append(columns, &col2)
	columns = append(columns, &col3)
	columns = append(columns, &col4)

	row := shim.Row{Columns: columns}
	ok, err := stub.InsertRow(TCTableName, row)

	if err != nil {
		return nil, fmt.Errorf("Insert a row in Tenancy_Contract table operation failed. %s", err)
	}

	if !ok {
		return nil, errors.New("Insert a row in Tenancy_Contract table operation failed. Row with given key already exists")
	}

	fmt.Println("TenancyContractID inserted is [" + tenancyContractID + "] ...")
	return []byte(tenancyContractID), err
}

//==============================================================================================================================
// deleteRowTenancyContract - Delete a row from the table Tenancy_Contract
//    input - key:value
//            "TenancyContractID" : "xxxxxx" <uuid>
//==============================================================================================================================
func (t *SimpleChaincode) deleteRowTenancyContract(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) != 2 {
		return nil, errors.New("Incorrect number of arguments, expecting 2")
	}

	if args[0] != "TenancyContractID" {
		return nil, errors.New("Incorrect argument name, expecting \"TenancyContractID\"")
	}

	tenancyContractID := args[1]

	err := stub.DeleteRow(
		TCTableName,
		[]shim.Column{shim.Column{Value: &shim.Column_String_{String_: tenancyContractID}}},
	)

	if err != nil {
		return nil, fmt.Errorf("Deleting a row in Tenancy_Contract table operation failed. %s", err)
	}

	return nil, nil
}

//==============================================================================================================================
// getTenancyContract - Query a row in the table Tenancy_Contract
//    input  - key:value
//						 "TenancyContractID"  : "T000100"
//		output - row
//==============================================================================================================================
func (t *SimpleChaincode) getTenancyContract(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) != 2 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2, \"TenancyContractID\" : \"xxxxxx\"")
	}

	if args[0] != "TenancyContractID" {
		return nil, errors.New("Unsupoprted query arguments [" + args[0] + "]")
	}

	fmt.Println("Start to query a TenancyContract ...")

	tenancyContractID := args[1]

	var columns []shim.Column
	keyCol1 := shim.Column{Value: &shim.Column_String_{String_: tenancyContractID}}

	columns = append(columns, keyCol1)

	row, err := stub.GetRow(TCTableName, columns)

	if err != nil {
		return nil, fmt.Errorf("Query a Tenancy Contract (TenancyContractID = %s) failed ", tenancyContractID)
	}

	if len(row.Columns) == 0 {
		return nil, errors.New("Tenancy Contract was NOT found ")
	}

	fmt.Println("Query a Tenancy Contract (TenancyContractID = " + tenancyContractID + " successfully ...")

	// Convert to the structure TenancyContract, the returns would be key1:value1,key2:value2,key3:value3, ...
	tContract := &TenancyContract{
		row.Columns[0].GetString_(),
		row.Columns[1].GetString_(),
		row.Columns[2].GetString_(),
		row.Columns[3].GetString_(),
	}

	jsonRow, err := json.Marshal(tContract)

	if err != nil {
		return nil, errors.New("getTenancyContract() json marshal error")
	}

	fmt.Println("End to query a TenancyContract ...")
	return jsonRow, nil
}

//==============================================================================================================================
// getTenancyContractsByID - Query rows in the table Tenancy_Contract owned by a specific ID
//    input  - key:value
//						 "LandlordID"  : "L000001"
//						 or
//						 "TenantID"    : "T000100"
//		output - rows
//==============================================================================================================================
func (t *SimpleChaincode) getTenancyContractsByID(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) != 2 {
		return nil, errors.New("Incorrect number of arguments. Expecting 2, \"LandlordID\" or \"TenantID\"")
	}

	fmt.Println("Start to query TenancyContractsByID ...")

	var colName string
	var colValue string

	colName = args[0]
	colValue = args[1]

	if colName != "LandlordID" && colName != "TenantID" {
		return nil, errors.New("Unsupoprted query arguments + [" + colName + "]")
	}

	var emptyArgs []string

	jsonTContracts, err := t.getAllTenancyContracts(stub, emptyArgs)

	if err != nil {
		return nil, fmt.Errorf("Query TenancyContractsByID failed. %s", err)
	}

	var allTContracts []TenancyContract
	unMarError := json.Unmarshal(jsonTContracts, &allTContracts)

	if unMarError != nil {
		return nil, fmt.Errorf("Error unmarshalling TenancyContracts: %s", err)
	}

	var returnTContracts []TenancyContract
	for i := 0; i < len(allTContracts); i = i + 1 {
		tContract := allTContracts[i]

		if colName == "LandlordID" {
			if tContract.LandlordID == colValue {
				returnTContracts = append(returnTContracts, tContract)
			}
		} else if colName == "TenantID" {
			if tContract.TenantID == colValue {
				returnTContracts = append(returnTContracts, tContract)
			}
		}
	}

	jsonReturnTContracts, jsonErr := json.Marshal(returnTContracts)
	if jsonErr != nil {
		return nil, fmt.Errorf("Query TenancyContractsIDs failed. Error marshaling JSON: %s", jsonErr)
	}

	fmt.Println("End to query TenancyContractsByID ...")

	return jsonReturnTContracts, nil
}

//==============================================================================================================================
// getAllTenancyContracts - Query all rows in the table Insurance_Contract
//		input  - nil
//
//		output - rows in JSON
//==============================================================================================================================
func (t *SimpleChaincode) getAllTenancyContracts(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	if len(args) > 0 {
		return nil, errors.New("Incorrect number of arguments. Expecting 0, no input needed ")
	}

	fmt.Println("Start to query All TenancyContracts ...")

	var columns []shim.Column

	rowChannel, err := stub.GetRows(TCTableName, columns)

	if err != nil {
		return nil, fmt.Errorf("Query all Tenancy Contracts failed. %s", err)
	}

	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	var tContracts []TenancyContract

	for i := 0; i < len(rows); i = i + 1 {
		oneRow := rows[i]
		tContract := &TenancyContract{
			oneRow.Columns[0].GetString_(),
			oneRow.Columns[1].GetString_(),
			oneRow.Columns[2].GetString_(),
			oneRow.Columns[3].GetString_(),
		}
		tContracts = append(tContracts, *tContract)
	}

	jsonRows, err := json.Marshal(tContracts)
	if err != nil {
		return nil, fmt.Errorf("Query all Tenancy Contracts failed. Error marshaling JSON: %s", err)
	}

	fmt.Println("End to query All TenancyContracts ...")

	return jsonRows, nil
}

//==============================================================================================================================
// testCert
//
//==============================================================================================================================

func (t *SimpleChaincode) testCert(stub shim.ChaincodeStubInterface, args []string) (bool, []byte, error) {

	myLogger.Info("Start to test certificate ...")

	metadata, err := stub.GetCallerMetadata()

	if err != nil {
		myLogger.Info("Failed getting metadata")
	}

	if len(metadata) == 0 {
		myLogger.Info("Invalid certificate. Empty")
	}

	myLogger.Info("metadata: " + string(metadata))
	// rbacMetadata := new(RBACMetadata)

	// _, err = asn1.Unmarshal(metadata, rbacMetadata)

	// if err != nil {
	// 	return false, nil, fmt.Errorf("Failed unmarshalling metadata [%s]", err)
	// }

	// Verify signature
	payload, err := stub.GetPayload()
	if err != nil {
		return false, nil, errors.New("Failed getting payload")
	}

	binding, err := stub.GetBinding()
	if err != nil {
		return false, nil, errors.New("Failed getting binding")
	}

	// myLogger.Info("passed certificate [% x]", rbacMetadata.Cert)
	// myLogger.Info("passed sigma [% x]", rbacMetadata.Sigma)
	myLogger.Info("passed payload [% x]", payload)
	myLogger.Info("passed binding [% x]", binding)

	// ok, err := stub.VerifySignature(
	// 	rbacMetadata.Cert,
	// 	rbacMetadata.Sigma,
	// 	append(rbacMetadata.Cert, append(payload, binding...)...),
	// )
	// if err != nil {
	// 	return false, nil, fmt.Errorf("Failed verifying signature [%s]", err)
	// }
	// if !ok {
	// 	return false, nil, fmt.Errorf("Signature is not valid")
	// }

	// myLogger.Info("Signature verified. Check for role...")

	return true, nil, nil
}

func (t *SimpleChaincode) checkCert(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {

	myLogger.Info("Start to check certificate ...")

	ok, _, err := t.testCert(stub, args)

	if err != nil {
		return nil, fmt.Errorf("Failed checking role [%s]", err)
	}
	if !ok {
		return nil, errors.New("The invoker does not have the required roles.")
	}

	myLogger.Info("End to check certificate ...")

	return nil, nil
}

func main() {
	err := shim.Start(new(SimpleChaincode))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode: %s", err)
	}
}
