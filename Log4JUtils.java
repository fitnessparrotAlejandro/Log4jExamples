package com.omv.endur.appInterface.SAPCreditInterface.util;

import com.olf.openjvs.DBUserTable;
import com.olf.openjvs.DBaseTable;
import com.olf.openjvs.OCalendar;
import com.olf.openjvs.ODateTime;
import com.olf.openjvs.OException;
import com.olf.openjvs.Ref;
import com.olf.openjvs.Table;
import com.olf.openjvs.Transaction;
import com.olf.openjvs.Util;
import com.olf.openjvs.enums.COL_FORMAT_TYPE_ENUM;
import com.olf.openjvs.enums.COL_TYPE_ENUM;
import com.olf.openjvs.enums.DATE_FORMAT;
import com.olf.openjvs.enums.OLF_RETURN_CODE;
import com.olf.openjvs.enums.SHM_USR_TABLES_ENUM;
import com.olf.openjvs.enums.TRANF_FIELD;
import com.omv.endur.appInterface.SAPCreditInterfaceEast.constants.SapCreditConstRepo;
import com.omv.endur.util.TableUtil;
import com.openlink.util.logging.PluginLog;

/**
 * <h1>Project: SAPCreditInterfaceEast</h1>
 * <h2>Author: EMC, 08.06.2021; Roberto Magistrelli</h2>
 * <h3>Description</h3>
 * <p>Class used for general static utility functions</p>
 */

public final class EndurUtils {
	
	/**
	* Disable instantiation
	*  
	*/
	private EndurUtils(){}
	
	/**
	 * Returns ODateTime according to the current server timestamp.
	 *
	 * @return ODateTime
	 * @throws OException
	 */
	public static ODateTime getCurrentServerDateTime() throws OException {

		ODateTime timeStamp = null;
	    timeStamp = ODateTime.dtNew();
		timeStamp.setDate(OCalendar.getServerDate());
		timeStamp.setTime(Util.timeGetServerTime());
		return timeStamp;
		
	}

	/**
	 * Converts int Date to ODateTime
	 *
	 * @param intDate
	 * @return ODateTime
	 * @throws OException
	 */
	public static ODateTime getODateFromInt(int intDate) throws OException {

		ODateTime rtVal = null;
		rtVal = ODateTime.dtNew();
		rtVal.setDate(intDate);
		return rtVal;
		
	}

	/**
	 * Executes DBaseTable.execISql and checks for errors
	 *
	 * @param tblData
	 * @param sql
	 * @return N/A
	 * @throws Exception
	 */
	public static void execISql(Table tblData, String sql) throws Exception{

		int rtVal = 0;
		
		try {
			
			rtVal = DBaseTable.execISql(tblData, sql);
			
		} finally {
			if (rtVal != OLF_RETURN_CODE.OLF_RETURN_SUCCEED.toInt()) {
				throw new Exception(DBUserTable.dbRetrieveErrorInfo(rtVal, sql));
			}
		}
	}
	
	/**
	 * Gets template tran_num based off reference field being populated and unique 
	 *
	 * @param reference
	 * @return N/A
	 * @throws Exception
	 */
	public static int getTemplateID(String reference) throws Exception{

		Table tblDataRS = Util.NULL_TABLE;
		int rtVal = -1;
		
		try {
			tblDataRS = Table.tableNew();
			
			String sql = "select 	tran_num " + 
					     " from  	ab_tran " +
						 " where 	reference = '" + reference + "'" + 
						 " and 		trade_flag = 1"	+
						 " and 		tran_status = 15";
			
			EndurUtils.execISql(tblDataRS, sql);
			
			if (tblDataRS.getNumRows() == 1){
				rtVal = tblDataRS.getInt(1, 1);
			} else if (tblDataRS.getNumRows() > 1) {
				throw new Exception("Duplicate template found, reference:"  + reference);
			} else if (tblDataRS.getNumRows() == 0) {
				throw new Exception("Template not found, reference:"  + reference);
			}
			
		} catch (Exception e) {
			throw(e);
		} finally {
			TableUtil.destroyTables(tblDataRS);
		}
		
		return rtVal;
		
	}
	
	/**
	 * Gets the Endur party_id for a Business Unit from the SAP id
	 *
	 * @param sapId
	 * @return party_id
	 * @throws Exception
	 */
	public static int getEndurExternalBunitFromSapIdEast(int sapId) throws Exception {
		return getEndurPartyIdFromSapIdEast(sapId, 1);
	}
	
	/**
	 * Gets the Endur party_id for a Legal Entity from the SAP id
	 *
	 * @param sapId
	 * @return party_id
	 * @throws Exception
	 */
	public static int getEndurExternalLentityFromSapIdEast(int sapId) throws Exception {
		return getEndurPartyIdFromSapIdEast(sapId, 0);
	}
	
	/**
	 * Gets the Endur party_id for a Legal Entity / Business Unit from the SAP id
	 *
	 * @param sapId
	 * @param partyClass 1 = Business Unit, 0 = Legal Entity 
	 * @return party_id
	 * @throws Exception
	 */
	private static int getEndurPartyIdFromSapIdEast(int sapId, int partyClass) throws Exception {
		int rtVal = -1;
		Table tblData = Util.NULL_TABLE;
		try {
			tblData = Table.tableNew();
			
			String sql = " select 	p.party_id " +  
						 " from     party p, party_info i, party_info_types t " +
						 " where 	p.party_id = i.party_id " + 
						 " and 		i.type_id = t.type_id " +
						 " and 		t.type_name = '" + SapCreditConstRepo.PARTY_INFO_SAP_ID_EAST + "' " + 
						 " and 		p.party_class = " + partyClass +
						 " and 		i.value = '" + sapId + "' order by p.party_id desc";
			
			EndurUtils.execISql(tblData, sql);
			
			if (tblData.getNumRows() > 1) {
				throw new OException("Duplicate party found for 'SAP ID East' = " + sapId);
			} else if (tblData.getNumRows() < 1) {
				throw new OException("No External party found for 'SAP ID East' = " + sapId);
			}
			
			rtVal = tblData.getInt(1, 1);
			
		} catch (Exception e) {
			throw(e);
		} finally {
			TableUtil.destroyTables(tblData);
		}
		return rtVal;
	}

	/**
	 * Checks to see if party_id for a Legal Entity / Business Unit exists for processing
	 *
	 * @param sapId
	 * @param partyClass 1 = Business Unit, 0 = Legal Entity 
	 * @return party_id
	 * @throws Exception
	 */
	public static boolean doesSapIdEastExist(int sapId, int partyClass) throws Exception {
		boolean rtVal = false;
		Table tblData = Util.NULL_TABLE;
		try {
			tblData = Table.tableNew();
			
			String sql = " select 	p.party_id " +  
						 " from     party p, party_info i, party_info_types t " +
						 " where 	p.party_id = i.party_id " + 
						 " and 		i.type_id = t.type_id " +
						 " and 		t.type_name = '" + SapCreditConstRepo.PARTY_INFO_SAP_ID_EAST + "' " + 
						 " and 		p.party_class = " + partyClass +
						 " and 		i.value = '" + sapId + "' order by p.party_id desc";
			
			EndurUtils.execISql(tblData, sql);
			
			if (tblData.getNumRows() == 1) {
				rtVal = true;
			}
			
		} catch (Exception e) {
			throw(e);
		} finally {
			TableUtil.destroyTables(tblData);
		}
		return rtVal;
	}
	
	/**
	 * Searches for an authorised party agreement between the Internal LE and the External LE on the deal and returns the AgreementId if it exists
	 * 
	 * @param tran The currently processing transaction
	 * @param Party Agreement Name
	 * @return AgreementId
	 * @throws Exception 
	 */
	public static int getAgreementId(Transaction tran, String agreementName) throws Exception {

		Table partyAgreementsTbl = Util.NULL_TABLE;
		int rtVal = -1;
		
		try {
			
			partyAgreementsTbl = Table.tableNew();
			String internalLE = tran.getField(TRANF_FIELD.TRANF_INTERNAL_LENTITY.toInt(), 0);
			String externalLE = tran.getField(TRANF_FIELD.TRANF_EXTERNAL_LENTITY.toInt(), 0);
			
			String sql = "select pa.party_agreement_id" 
						+ " from party_agreement pa"
						+ " , party ext_party"
						+ " , party int_party"
						+ " , agreement ag"
						+ " where ext_party.party_id = pa.ext_party_id"
						+ " and int_party.party_id = pa.int_party_id"
						+ " and ext_party.short_name = '" + externalLE + "'"
						+ " and int_party.short_name = '" + internalLE + "'"
						+ " and pa.doc_status = 1" 
						+ " and pa.agreement_id = ag.agreement_id "
						+ " and ag.agreement_name = '" + agreementName + "'";
			
			EndurUtils.execISql(partyAgreementsTbl, sql);
			
			if (partyAgreementsTbl.getNumRows() == 0) {
				throw new OException("No party agreement found between Int LE '" + internalLE + "' and Ext LE '" + externalLE + "' on agreement " + agreementName);
			}
			
			rtVal = partyAgreementsTbl.getInt(1, 1);
			
		} catch (Exception e) {
			throw(e);
		} finally {
			TableUtil.destroyTables(partyAgreementsTbl);
		}
		
		return rtVal;
	}
	
	/**
	 * Searches for an authorised party agreement between the Internal LE and the External LE and the Instrument Type on the deal and returns the AgreementId if it exists
	 * 
	 * @param tran The currently processing transaction
	 * @param Party Agreement Name
	 * @return AgreementId
	 * @throws Exception 
	 */
	public static int getAgreementId(Transaction tran) throws Exception {

		Table partyAgreementsTbl = Util.NULL_TABLE;
		int rtVal = -1;
		
		try {
			
			partyAgreementsTbl = Table.tableNew();
			String internalLE = tran.getField(TRANF_FIELD.TRANF_INTERNAL_LENTITY.toInt(), 0);
			String externalLE = tran.getField(TRANF_FIELD.TRANF_EXTERNAL_LENTITY.toInt(), 0);
			int instrument = tran.getFieldInt(TRANF_FIELD.TRANF_INS_TYPE.toInt(), 0);
			
			String sql = "select pa.party_agreement_id" 
						+ " from party_agreement pa"
						+ " , party ext_party"
						+ " , party int_party"
						+ " , agreement ag"
						+ " , agreement_ins ai"
						+ " where ext_party.party_id = pa.ext_party_id"
						+ " and int_party.party_id = pa.int_party_id"
						+ " and ext_party.short_name = '" + externalLE + "'"
						+ " and int_party.short_name = '" + internalLE + "'"
						+ " and pa.doc_status = 1" 
						+ " and pa.agreement_id = ag.agreement_id "
						+ " and ai.agreement_id = pa.agreement_id "
						+ " and ai.ins_type = " + instrument;
			
			EndurUtils.execISql(partyAgreementsTbl, sql);
			
			if (partyAgreementsTbl.getNumRows() == 0) {
				throw new OException("No party agreement found between Int LE '" + internalLE + "' and Ext LE '" + externalLE + "' on ins_type " + instrument);
			} else if (partyAgreementsTbl.getNumRows() > 1){
				throw new OException("Too Many party agreements found between Int LE '" + internalLE + "' and Ext LE '" + externalLE + "' on ins_type " + instrument);
			}
			
			rtVal = partyAgreementsTbl.getInt(1, 1);
			
		} catch (Exception e) {
			throw(e);
		} finally {
			TableUtil.destroyTables(partyAgreementsTbl);
		}
		
		return rtVal;
	}
	
	/**
	 * Formats a double with "," instead of "." for regional differences
	 * 
	 * @param dblValue
	 * @param decimalPlaces
	 * @return e.g. String = 10000,00
	 * @throws Exception 
	 */
	public static String formatDouble(double dblValue, int decimalPlaces) throws Exception {
		
		String rtVal = "0,00";
		double dblVal = 0;

		dblVal = com.olf.openjvs.Math.round(dblValue, decimalPlaces);
	    		
	    rtVal = Double.toString(dblVal);
        rtVal = rtVal.replace(".", ",");
		
		return rtVal;
	}
	
	/**
	 * Converts an int Date to ODateTime and then to a String and checks its not past year 2099
	 * 
	 * @param intDate
	 * @return Date in String format
	 * @throws Exception 
	 */
	public static String formatDateAndForce2099(int intDate) throws Exception {
		
		String rtVal = "";

		if (intDate > 73048){
			intDate = 73048;
		}
		rtVal = OCalendar.formatJd(intDate);

		return rtVal;
	}
	
	/**
	 * Gets the current Business date from the system_dates table , not the user session, in String format
	 * 
	 * @return String Date
	 * @throws Exception 
	 */
	public static String getStrDateBusinessDateForDbAccess() throws Exception{
		return OCalendar.formatJdForDbAccess(Util.getBusinessDate());
	}
	
	/**
	 * Gets full stack trace and error message 
	 * 
	 * @param The error thrown 
	 * @return String of the error message
	 * @throws Exception
	 */
	public static String getFullErrorMessage(Throwable ex) throws Exception {
		String rtVal = "";
		
		if(ex.getMessage() != null) {
			if(ex.getCause() != null) {
				rtVal = ex.getCause().getClass().getCanonicalName() + ": " + ex.getCause().getMessage() + "\n";
			} else {
				rtVal = ex.getClass().getCanonicalName() + ": " + ex.getMessage() + "\n";	
			}
		} else {
			if(ex.getCause() != null) {
				rtVal = ex.getCause().getClass().getCanonicalName() + ": " + ex.getCause().getMessage() + "\n";
			} else {	
				rtVal = ex.getClass().getCanonicalName() + "\n";
			}
		}
		
		StackTraceElement[] stackTrace = ( ex.getCause() != null ) ? ex.getCause().getStackTrace() : 
					                                                 ex.getStackTrace();
		
		for ( StackTraceElement elem : stackTrace ) {
			rtVal += String.format("%s:%s:%d:%s", elem.getClassName(), elem.getFileName(), elem.getLineNumber(), elem.getMethodName()) + "\n";
		} 
		
		return rtVal; 
	}
	  
	/**
	 * Get the script id from its name from dir_node - used for grid processing normally
	 * 
	 * @param scriptName - the script name
	 * @return the script id
	 * @throws OException 
	 */
	public static int getScriptId(String scriptName) throws Exception {
		
		PluginLog.debug("Get the script id of script " + scriptName);
		
		int rtVal = -1;
		Table tblData = Util.NULL_TABLE;
		
		String sql = "SELECT client_data_id FROM dir_node WHERE node_name = '" + scriptName + "'";
		
		try {
			
			tblData = Table.tableNew();
			EndurUtils.execISql(tblData, sql);
			if (tblData.getNumRows() == 1) {
				rtVal = tblData.getInt(1, 1);
			}
			if (rtVal < 1) {
				throw new OException("Returned script id of script " + scriptName + " is invalid");
			}
			
		} catch (Exception e) {
			throw(e);
		} finally {
			TableUtil.destroyTables(tblData);
		}
		
		PluginLog.debug("The script id is " + rtVal);
		return rtVal;
	}
	
	/**
	 * Converts Table into HTML format typically for email.
	 * 
	 * @param Title for HTML 
	 * @param Table
	 * @return "<html> ... <html\>"
	 * @throws Exception 
	 */
	public static String tableToHtml(String title, Table data) throws Exception {

		StringBuffer sb = new StringBuffer();

		sb.append("<HTML>\n");
		sb.append("<HEAD>\n");
		sb.append("<TITLE>");
		sb.append( title );
		sb.append("</TITLE>\n");
		sb.append("<!-- GNU Terry Pratchett -->\n");
		sb.append("<style type='text/css'>\n");
		sb.append("<!--\n");
		sb.append("body { background: #DEF; font: 8pt Verdana, Arial, sans-serif; }\n");
		sb.append("h2 { font: bold 12pt Verdana, Arial, sans—serif; }\n");
		sb.append("table { background: #BAC; padding: 1px; margin: 1px; }\n");
		sb.append("th { color: #FFF; background: #369; font: bold lOpt Verdana, Arial, sans-serif; padding: 1px; margin: 1px; }\n");
		sb.append("td { background: #FFF; font: lOpt Verdana, Arial, sans-serif; padding: 1px; margin: 1px; }\n");
		sb.append("//-->\n");
		sb.append("</style>\n");
		sb.append("</HEAD>\n");
		sb.append("<BODY>\n");
		
		if( data == null ) {
			sb.append("<H2>No Data</H2>\n");
		} else {
			sb.append("<H2>");
			sb.append( data.getTableName() );
			sb.append("</H2>\n");
			sb.append("<table>\n");
			
			// Format column headings
			int n = data.getNumCols();
			sb.append("<tr>");
			for( int i = 1; i <= n; i++ ) {
				sb.append("<th>");
				sb.append(getTitle(data, i));
				sb.append("</th>");
			}
			sb.append("</tr>\n");
			
			// Format data
			int m = data.getNumRows();
			if( m > 0 ) {
				for( int j = 1; j <= m; j++ ) {
					sb.append("<tr>");
					for( int i = 1; i <= n; i++ ) {
						if(data.getColName(i).endsWith("_")) {
						   sb.append("<td align=right>");
						} else {
						   sb.append("<td>");
						}
						sb.append(getString(data, i, j));
						sb.append("</td>");
					}
					sb.append("</tr>\n");
				}
			} else {
				sb.append("<tr><td colspan=\"");
				sb.append(n);
				sb.append("\" align=center><b>No Data</b></td></tr>\n");
			}
			sb.append("</table>\n");
		}
		
		sb.append("</BODY>\n");
		sb.append("</HTML>\n");

		return sb.toString();
	}
	
	/**
	 * Converts any table coltype to formatted String   
	 * 
	 * @param Table
	 * @param col
	 * @param row
	 * @return String value
	 * @throws Exception 
	 */
	public static String getString(Table data, int col, int row) throws Exception {
	    
		Table 	ltblTemp =  Util.NULL_TABLE;
	    String 	lsVal;
	    double 	ldAmount;
	    int 	liVal, liFmt;

	    if (data == null) return "";

	    if (col < 1 || col > data.getNumCols()) return "";

	    COL_TYPE_ENUM colType = COL_TYPE_ENUM.fromInt(data.getColType(col));
	    
	    switch (colType) {
	    case COL_STRING:
	    	lsVal = data.getString(col, row);
	    	break;
	    case COL_INT:
	    	liVal = data.getInt(col, row);
	    	lsVal = Integer.toString(liVal);
	    	liFmt = data.getColFormat(col);
	    	if (liFmt == COL_FORMAT_TYPE_ENUM.FMT_REF.toInt()) {
	    		int liRef = data.getColFormatParam(col, 0);
	    		if (liRef >= 0) {
	    			lsVal = Ref.getName(SHM_USR_TABLES_ENUM.fromInt(liRef), liVal);
	    		}
	    	} else {
	    		if (liFmt == COL_FORMAT_TYPE_ENUM.FMT_DATE.toInt()) {
	    			lsVal = OCalendar.formatJd( liVal, getDateFormat(data, col));
	    		}
	    	}
	    	break;
	    case COL_INT64:
	    	lsVal = Long.toString(data.getInt64(col, row));
	    	break;
	    case COL_DOUBLE:
	    	ldAmount = data.getDouble(col, row);
	    	lsVal = formatDouble(ldAmount, 2);
	    	break;
	    case COL_DATE_TIME:
	    	lsVal = OCalendar.formatJdForDbAccess(data.getDate(col, row));
	    	break;
	    case COL_TABLE:
	    	ltblTemp = data.getTable(col, row);
            lsVal = (ltblTemp == null) ? "" : ltblTemp.getTableName();
            break;
	    case COL_CLOB:
	    	lsVal = data.getClob(col, row);
	    	break;
	    default:
	    	throw new Exception("Invalid column type :" + colType);
	    }
	    
		if (lsVal == null) {
			lsVal = "";
		}
		return lsVal;
	}
	
	/**
	 * Gets format of col on table 
	 * 
	 * @param Table
	 * @param col
	 * @return DATE_FORMAT
	 * @throws OException 
	 */
	private static DATE_FORMAT getDateFormat(Table data, int colld) throws OException {
	    DATE_FORMAT dateFmt;
	    try {
	        dateFmt = DATE_FORMAT.fromInt(data.getColFormatParam(colld, 0));
	    } catch (Exception ex) {
	        return DATE_FORMAT.DATE_FORMAT_DMLY_NOSLASH;
	    }
	    return dateFmt;
	}
	
	/**
	* Returns the title for a column, or generates one from the column name if no title is set
	* @param data
	* @param colld
	* @return title of column
	*/
	public static String getTitle( Table data, int colld ) throws Exception {
		return getTitle(data, data.getColName(colld));
	}
	
	/**
	* Returns the title for a column, or generates one from the column name if no title is set
	* @param data
	* @param colName
	* @return title of column
	*/
	public static String getTitle( Table data, String colName ) throws Exception {
		String title = data.getColTitle(colName);
		if( !title.isEmpty()) {
			return title;
		}
		return getTitle(colName);
	}

	/**
	* Returns a nicely formated string to use as a title from column name (for tables where column titles aren't set)<br>
	* "this_column_name" becomes "This Column Name"
	* @param name
	* @return
	*/
	public static String getTitle( String name) throws Exception
	{
		StringBuffer sb = new StringBuffer();
		
		boolean uc = true;
		int n = name.length();
		for( int i = 0; i < n; i++) {
			char c = name.charAt(i);
			if( c == '_') {
				sb.append(' ');
				uc = true;
			}
			else {
				if( c == Character.toUpperCase(c) ) {
					if(!uc) sb.append(' ');
					sb.append(c);
				} else {
					sb.append( uc ? Character.toUpperCase(c) : Character.toLowerCase(c));
				}
				uc = false;
			}
		}
		
		return sb.toString();
		
	}
	
	/**
	* Returns the FXRate for a currency from the idx_historical_fx_rates table
	* @param fxRateDate
	* @param currency
	* @param referenceCurrency
	* @return FXRate = Zero returned if not Found , no exception raised.
	*/
	public static double getFXRate(int fxRateDate, int currency, int referenceCurrency) throws Exception{

		double rtVal = 0;
		Table tblData = Util.NULL_TABLE;
		try {
			
			tblData = Table.tableNew();
			
			String sql = " select  fx_rate_mid as ohd_fx_rate_mid, last_update " +  
						 " from    idx_historical_fx_rates " +
						 " where   currency_id = " + currency +
						 " and     reference_currency_id = " + referenceCurrency + 
						 " and 	   data_set_type = 1 " + 	
						 " and     fx_rate_date = '" +  OCalendar.formatJdForDbAccess(fxRateDate) + "'" +
						 " order by last_update desc ";
			
			EndurUtils.execISql(tblData, sql);
			
			if (tblData.getNumRows() == 1) {
				rtVal = tblData.getDouble(1, 1);
			}
			
		} catch (Exception e) {
			throw(e);
		} finally {
			TableUtil.destroyTables(tblData);
		}
		return rtVal;
	}
	
	/**
	* Returns the FXRate for a currency from the idx_historical_fx_rates table for the LGB
	* @param currency
	* @param referenceCurrency
	* @return FXRate = Zero returned if not Found , no exception raised.
	*/
	public static double getLgbHistoricalFXRate(int currency, int referenceCurrency) throws Exception{
		return getFXRate(OCalendar.getLgbdForCurrency(Util.getBusinessDate(), SapCreditConstRepo.EUR), SapCreditConstRepo.RON, SapCreditConstRepo.EUR); 
	}
	
	/**
	* Get RON to EUR FXRate for last good business day
	* @return FXRate = Return Exception if not Found
	 * @throws Exception 
	*/	
	public static double getlgbdFxRateRON() throws Exception {
		double rtVal = 0;

		rtVal = getLgbHistoricalFXRate(SapCreditConstRepo.RON,SapCreditConstRepo.EUR);
		
		if (rtVal == 0) {
			throw new OException("No FX Rate found in historical prices for RON to EUR for " + OCalendar.getLgbdForCurrency(Util.getBusinessDate(), SapCreditConstRepo.EUR));
		}
		
		return rtVal;
	}
	
}
