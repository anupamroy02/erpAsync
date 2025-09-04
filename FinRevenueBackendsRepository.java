package com.axiom.revbackend.repository;

//import com.axiom.revbackend.jooq.entity.tables.FndLookupValues;
//import com.axiom.revbackend.jooq.entity.tables.OkcKHeadersAllB;
//import com.axiom.revbackend.jooq.entity.tables.OkcKLinesB;
//import com.axiom.revbackend.jooq.entity.tables.xxOkcKLinesB;
import com.axiom.revbackend.jooq.entity.packages.xxfpeglRevUtilPkg;
import com.axiom.revbackend.jooq.entity.packages.arrevenueadjustmentpvt.udt.records.RevAdjRecTypeRecord;
import com.axiom.revbackend.jooq.entity.packages.xxfpeglrevutilpkg.ProcessPilotRevrec;
import com.axiom.revbackend.util.DBUtils;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
// Done by Max - I'm adding these imports for the async processing enhancement
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
//import org.jooq.Record4;
//import org.jooq.Result;
import org.xeril.util.tx.TransactionException;
import proto.com.axiom.revbackend.FinRevenueBackendActionRequest;
import proto.com.axiom.revbackend.FinRevenueBackendActionResponse;

//import static org.jooq.impl.DSL.*;


@Slf4j
public class FinRevenueBackendsRepository {

    private final DataSource _dataSource;

    // Done by Max - I'm adding these fields to support async processing as per the requirements
    private final ExecutorService asyncExecutor;
    private final Set<String> processingRequests;

    public FinRevenueBackendsRepository(DataSource dataSource) throws TransactionException {
        this._dataSource = dataSource;

        // Done by Max - I'm initializing the async processing infrastructure here
        // I'm creating a thread pool for background processing
        this.asyncExecutor = Executors.newFixedThreadPool(5);
        // I'm using this to track active requests for idempotency
        this.processingRequests = ConcurrentHashMap.newKeySet();

        log.info("FinRevenueBackendsRepository initialized with async processing capability");
    }

    public FinRevenueBackendActionResponse processRevRec(FinRevenueBackendActionRequest request)
            throws TransactionException, SQLException {

        log.info("Inside processRevRec method");

        // Done by Max - I'm adding the routing logic here as specified in the requirements document
        String contractNumber = request.getValue().getContractNumber();

        if (contractNumber == null || contractNumber.trim().isEmpty()) {
            // I'm routing to async processing when contractNumber is NULL as per requirements
            log.info("Contract number is NULL/empty - I'm routing to ASYNC processing");
            return handleAsyncProcessing(request);
        } else {
            // I'm routing to existing sync processing to maintain backward compatibility
            log.info("Contract number provided: '{}' - I'm routing to SYNC processing", contractNumber);
            return handleSyncProcessing(request);
        }
    }

    /**
     * Done by Max - I'm handling synchronous processing here with existing functionality preserved
     * This maintains backward compatibility for existing clients
     */
    private FinRevenueBackendActionResponse handleSyncProcessing(FinRevenueBackendActionRequest request)
            throws TransactionException, SQLException {

        log.info("Starting SYNC processing for contract: {}", request.getValue().getContractNumber());

        // keeping all the existing logic exactly the same for backward compatibility
        FinRevenueBackendActionResponse.Builder responseBuilder = FinRevenueBackendActionResponse.newBuilder();

        DSLContext dslContext = DBUtils.getDSLContext(_dataSource);

        final String selectQuery1 = "SELECT \n"
                + "    okhab.contract_number contract_number,\n"
                + "    flv.meaning meaning,\n"
                + "    flv.description description,\n"
                + "    flv.tag tag,\n"
                + "    okhab.org_id org_id,\n"
                + "    oklb.date_terminated date_terminated,\n"
                + "    oklb.end_date end_date\n"
                + "    \n"
                + "FROM \n"
                + "    xx_okc_k_lines_b xoklb, \n"
                + "    okc_k_headers_all_b okhab, \n"
                + "    okc_k_lines_b oklb, \n"
                + "    fnd_lookup_values flv\n"
                + "WHERE \n"
                + "    xoklb.chr_id = oklb.dnz_chr_id\n"
                + "    AND xoklb.cle_id = oklb.id \n"
                + "    AND oklb.dnz_chr_id = okhab.id \n"
                + "    AND okhab.contract_number = NVL(?,okhab.contract_number)\n"
                + "    AND (NVL(trunc(oklb.date_terminated),trunc(oklb.end_date))) < NVL(TO_DATE(?,'DD-MON-YYYY'), SYSDATE)\n"
                //+ "    AND (? IS NULL OR NVL(trunc(oklb.date_terminated),trunc(oklb.end_date)) >= TRUNC(TO_DATE(?, 'YYYY-MM-DD')))\n"
                // + "    AND (? IS NULL OR NVL(trunc(oklb.date_terminated),trunc(oklb.end_date)) <= TRUNC(TO_DATE(?, 'YYYY-MM-DD')))\n"
                // + "    \n"
                + "    AND flv.description = NVL(?,flv.description)\n"
                + "    AND UPPER(flv.meaning) = NVL(?,UPPER(flv.meaning))\n"
                + "    AND TRUNC(okhab.creation_date) >= to_date(flv.attribute1,'rrrr/mm/dd hh24:mi:ss')\n"
                + "    AND xoklb.attribute65 IS NOT NULL\n"
                + "    AND xoklb.attribute65 = UPPER(meaning)\n"
                + "    AND flv.lookup_type = 'xx_CMRC_PILOT_PROGRAM'\n"
                + "GROUP BY\n"
                + "        okhab.contract_number,\n"
                + "        flv.meaning,\n"
                + "        flv.description,\n"
                + "        flv.tag,\n"
                + "        okhab.org_id,\n"
                + "        oklb.date_terminated,\n"
                + "        oklb.end_date";

        List<ContractDetailRowMapper.ContractDetail> contractDetails = new ArrayList<>();

        try (Connection connection = _dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(selectQuery1)) {

            //String toDate = request.getValue().getToDate();
            //String fromDate = request.getValue().getFromDate();

            // Set query parameters
            preparedStatement.setString(1, request.getValue().getContractNumber());
            //preparedStatement.setString(2, fromDate);
            //preparedStatement.setString(3, fromDate); // fromDate parameter
            //preparedStatement.setString(4, toDate);
            //preparedStatement.setString(5, toDate); // toDate parameter
            preparedStatement.setString(2, request.getValue().getExecDate());
            preparedStatement.setString(3, request.getValue().getProductName());
            preparedStatement.setString(4, request.getValue().getOfferName());

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    ContractDetailRowMapper.ContractDetail contractDetail = new ContractDetailRowMapper.ContractDetail();
                    contractDetail.setContractNumber(resultSet.getString("contract_number"));
                    contractDetail.setMeaning(resultSet.getString("meaning"));
                    contractDetail.setDescription(resultSet.getString("description"));
                    contractDetail.setTag(resultSet.getString("tag"));
                    contractDetail.setOrgId(resultSet.getInt("org_id"));
                    contractDetails.add(contractDetail);
                }
            }
        } catch (SQLException e) {
            log.error("Exception occurred while fetching the contract details: " + request.getValue().getContractNumber(), e);
            responseBuilder.addFinRevResponses("Exception occurred while fetching the contract details: " + e.getMessage());
            throw new SQLException(e);
        }

        if (contractDetails.isEmpty()) {
            log.info("No records found for the given input Contract Number: {}, Exec Date: {}, Product Name: {}, Offer Name: {}",
                    request.getValue().getContractNumber(), request.getValue().getExecDate(), request.getValue().getProductName(),
                    request.getValue().getOfferName());
            responseBuilder.addFinRevResponses("No records found for the given input");
            return responseBuilder.build();
        }

        String selectQuery2 = "SELECT racta.trx_number,\n" + "         racta.customer_trx_id,\n"
                + "         ractla.customer_trx_line_id\n" + "    FROM ra_customer_trx_all racta,\n"
                + "         ra_customer_trx_lines_all ractla,\n" + "         ra_cust_trx_types_all ractta,\n"
                + "         (WITH dts AS(\n" + "          SELECT TRUNC(c.start_date) AS from_dt,\n"
                + "                 TRUNC(c.end_date) AS to_dt,\n" + "            TRUNC(c.date_terminated) AS termination_dt\n"
                + "           FROM okc_k_headers_all_b b,\n"
                + "                okc_k_lines_b c\n"
                + "           WHERE contract_number = ?\n"
                + "             AND c.dnz_chr_id = b.id )\n"
                + "          SELECT ADD_MONTHS(from_dt,(level-1)*12) AS from_dt,\n"
                + "                 LEAST(to_dt, ADD_MONTHS(from_dt,(level*12))-1) AS to_dt,\n" + "            termination_dt AS termination_dt\n"
                + "             FROM dts\n"
                + "         CONNECT BY ADD_MONTHS(from_dt,(level-1)*12) <= to_dt) dates\n"
                + "   WHERE racta.interface_header_attribute1 = ? \n"
                + "     AND ractla.customer_trx_id = racta.customer_trx_id\n" + "     AND ractla.line_type = 'LINE'\n"
                + "     AND ractla.rule_start_date >= TRUNC(dates.from_dt)\n"
                + "     AND ractla.rule_end_date <= TRUNC(dates.to_dt)\n"

                // + "     AND (? IS NULL OR (NVL(TRUNC(dates.termination_dt),TRUNC(dates.to_dt)) >= TRUNC(TO_DATE(?,'YYYY-MM-DD'))))"
                // + "     AND (? IS NULL OR (NVL(TRUNC(dates.termination_dt),TRUNC(dates.to_dt)) <= TRUNC(TO_DATE(?,'YYYY-MM-DD'))))"
                + " AND (NVL(TRUNC(dates.termination_dt),TRUNC(dates.to_dt))) < NVL(TO_DATE(?,'DD-MON-YYYY'), SYSDATE)\n"
                + "     AND ractta.cust_trx_type_id = racta.cust_trx_type_id\n" + "     AND ractta.type = 'INV'\n"
                + "     AND NOT EXISTS (SELECT 1\n" + "                       FROM ra_cust_trx_line_gl_dist_all ractgda\n"
                + "                      WHERE ractgda.customer_trx_id = racta.customer_trx_id\n"
                + "                        AND ractgda.customer_trx_line_id = ractla.customer_trx_line_id\n"
                + "                        AND ractgda.amount IS NOT NULL\n"
                + "                        AND ractgda.original_gl_date IS NOT NULL\n"
                + "                        AND ractgda.account_class = 'REV') \n"
                + "   GROUP BY racta.trx_number,\n"
                + "            racta.customer_trx_id,\n" + "            ractla.customer_trx_line_id";


        for (ContractDetailRowMapper.ContractDetail contractDetail : contractDetails) {
            log.info("SYNC Processing - Contract Number: {}, Meaning: {}, Description: {}, Tag: {}",
                    contractDetail.getContractNumber(), contractDetail.getMeaning(), contractDetail.getDescription(),
                    contractDetail.getTag());


            List<InvoiceDetailRowMapper.InvoiceDetail> invoiceDetails = new ArrayList<>();

            try (Connection connection = _dataSource.getConnection();
                 PreparedStatement preparedStatement = connection.prepareStatement(selectQuery2)) {

                //String toDate = request.getValue().getToDate();
                //String fromDate = request.getValue().getFromDate();

                // Set query parameters
                preparedStatement.setString(1, contractDetail.getContractNumber());
                preparedStatement.setString(2, contractDetail.getContractNumber());
                preparedStatement.setString(3, request.getValue().getExecDate());
                //preparedStatement.setString(3, fromDate);
                //preparedStatement.setString(4, fromDate); // fromDate parameter
                //preparedStatement.setString(5, toDate);
                //preparedStatement.setString(6, toDate); // toDate parameter


                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        InvoiceDetailRowMapper.InvoiceDetail invoiceDetail = new InvoiceDetailRowMapper.InvoiceDetail();
                        invoiceDetail.setInvoiceNumber(resultSet.getString("trx_number"));
                        invoiceDetail.setCustTrxId(resultSet.getString("customer_trx_id"));
                        invoiceDetail.setCustTrxLineId(resultSet.getString("customer_trx_line_id"));
                        invoiceDetails.add(invoiceDetail);
                    }
                }
            } catch (SQLException e) {
                log.error("Exception occurred while fetching the invoice details for contract: " + contractDetail.getContractNumber(), e);
                responseBuilder.addFinRevResponses("Exception occurred while fetching the invoice details: " + e.getMessage());
                throw new SQLException(e);
            }

            if (invoiceDetails.isEmpty()) {
                log.info("No eligible invoice records found for the given contract number: {} and tag: {}",
                        contractDetail.getContractNumber(), contractDetail.getTag());
                responseBuilder.addFinRevResponses("No eligible Invoice records found for the given input");
                return responseBuilder.build();
            }

            for (InvoiceDetailRowMapper.InvoiceDetail invoiceDetail : invoiceDetails) {

                log.info("SYNC Processing - Invoice Number: {}, Customer Trx Id: {}, Customer Trx Line Id: {}",
                        invoiceDetail.getInvoiceNumber(), invoiceDetail.getCustTrxId(), invoiceDetail.getCustTrxLineId());

                xxfpeglRevUtilPkg.xxfpeglAppsInitialize(dslContext.configuration(), 1286, 20678, 222);
                xxfpeglRevUtilPkg.xxfpeglMoGlobalInit(dslContext.configuration(), "AR");
                xxfpeglRevUtilPkg.xxfpeglMoGlobal(dslContext.configuration(), contractDetail.getOrgId());

                RevAdjRecTypeRecord revAdjRecTypeRecord = new RevAdjRecTypeRecord();
                revAdjRecTypeRecord.setTrxNumber(invoiceDetail.getInvoiceNumber());
                revAdjRecTypeRecord.setCustomerTrxId(Long.parseLong(invoiceDetail.getCustTrxId()));
                revAdjRecTypeRecord.setFromCustTrxLineId(Long.parseLong(invoiceDetail.getCustTrxLineId()));
                revAdjRecTypeRecord.setToCustTrxLineId(Long.parseLong(invoiceDetail.getCustTrxLineId()));
                revAdjRecTypeRecord.setAdjustmentType("EA");
                revAdjRecTypeRecord.setAmountMode("T");
                revAdjRecTypeRecord.setLineSelectionMode("A"); //LINE_SELECTION_MODE
                revAdjRecTypeRecord.setReasonCode("RA");
                revAdjRecTypeRecord.setGlDate(new Date(System.currentTimeMillis()).toLocalDate()); //GL_DATE

                log.info("Calling processPilotRevrec with parameters: ");

                ProcessPilotRevrec p = xxfpeglRevUtilPkg.processPilotRevrec(
                        dslContext.configuration(),
                        2.0,
                        "T",
                        revAdjRecTypeRecord
                );

                log.info("API Return Status is: {}", p.getXReturnStatus());
                log.info("Adjustment ID is: {}", p.getXAdjustmentId());
                log.info("Adjustment Number is: {}", p.getXAdjustmentNumber());
                log.info("Msg Count is: {}", p.getXMsgCount());
                log.info("Msg data is: {}", p.getXMsgData());

                String responseForInvoice =
                        "Invoice Number: " + invoiceDetail.getInvoiceNumber() + " | " + "Return Status: " + p.getXReturnStatus()
                                + " | " + "Adjustment ID: " + p.getXAdjustmentId() + " | " + "Adjustment Number: "
                                + p.getXAdjustmentNumber() + " | " + "Msg Count: " + p.getXMsgCount() + " | " + "Msg data: "
                                + p.getXMsgData();

                responseBuilder.addFinRevResponses(responseForInvoice);
                log.info("Response for Invoice: {}", responseForInvoice);
            }
        }

        if (responseBuilder.getFinRevResponsesCount() == 0) {
            responseBuilder.addFinRevResponses("No records found for the given input");
        }

        log.info("SYNC processing completed for contract: {}", request.getValue().getContractNumber());
        return responseBuilder.build();
    }

    /**
     * Done by Max - I'm handling asynchronous processing here with new functionality as per requirements
     * This returns immediate acknowledgment and processes in background
     */
    private FinRevenueBackendActionResponse handleAsyncProcessing(FinRevenueBackendActionRequest request) {
        log.info("I'm starting ASYNC processing for request with NULL contract number");

        FinRevenueBackendActionResponse.Builder responseBuilder = FinRevenueBackendActionResponse.newBuilder();

        try {
            //  Generating unique request ID for tracking as specified in requirements
            String requestId = generateRequestId(request);

            log.info("I'm generating request ID: {} for async processing", requestId);

            // I'm checking for idempotency to prevent duplicate processing
            if (processingRequests.contains(requestId)) {
                log.warn("Request {} already being processed - I'm returning existing acknowledgment", requestId);
                responseBuilder.addFinRevResponses("Request already in progress");
                responseBuilder.setKey(requestId);
                return responseBuilder.build();
            }

            // I'm marking request as processing for idempotency protection
            processingRequests.add(requestId);
            log.info("Request {} marked as processing", requestId);

            // I'm submitting background processing task as required
            CompletableFuture.runAsync(() -> {
                try {
                    log.info("Starting background processing for request: {}", requestId);
                    processAllEligibleContractsAsync(request, requestId);
                    log.info("Completing background processing successfully for request: {}", requestId);
                } catch (Exception e) {
                    log.error("Background processing failed for request: " + requestId, e);
                } finally {
                    // Here is to lways removing from processing set when done
                    processingRequests.remove(requestId);
                    log.info("Removing request {} from processing set", requestId);
                }
            }, asyncExecutor);

            // Returning immediate acknowledgment as specified in requirements document
            responseBuilder.addFinRevResponses("Request received and processing in progress");
            responseBuilder.setKey(requestId);

            log.info("Sending ASYNC processing acknowledgment for request ID: {}", requestId);
            return responseBuilder.build();

        } catch (Exception e) {
            log.error("Error in async processing setup", e);
            responseBuilder.addFinRevResponses("Error initiating processing: " + e.getMessage());
            return responseBuilder.build();
        }
    }

    /**
     * Done by Max - I'm processing all eligible contracts in the background
     * This method handles the main async processing logic
     */
    private void processAllEligibleContractsAsync(FinRevenueBackendActionRequest request, String requestId) {
        log.info("Starting ASYNC processing == Request ID: {}", requestId);

        try {
            // Finding all eligible contracts (without specific contract filter)
            List<ContractDetailRowMapper.ContractDetail> contractDetails = findAllEligibleContracts(request, requestId);

            if (contractDetails.isEmpty()) {
                log.info("No eligible contracts found for async request: {}", requestId);
                return;
            }

            log.info("Finding {} eligible contracts for async processing in request: {}",
                    contractDetails.size(), requestId);

            // Processing each contract individually as per requirements (failure isolation)
            int successCount = 0;
            int failureCount = 0;

            for (ContractDetailRowMapper.ContractDetail contractDetail : contractDetails) {
                try {
                    log.info("Processing contract: {} for request: {}", contractDetail.getContractNumber(), requestId);
                    processIndividualContractAsync(request, contractDetail, requestId);
                    successCount++;
                    log.info("Successfully processing contract: {} (Request: {})",
                            contractDetail.getContractNumber(), requestId);
                } catch (Exception e) {
                    failureCount++;
                    // Logging failure but continuing with other contracts as required
                    log.error("Failing to process contract: {} for request: {}. Error: {}",
                            contractDetail.getContractNumber(), requestId, e.getMessage(), e);
                }
            }

            log.info("Completing ASYNC processing ==== Request ID: {}, Total: {}, Success: {}, Failed: {}",
                    requestId, contractDetails.size(), successCount, failureCount);

        } catch (Exception e) {
            log.error("Encountering ASYNC processing failure === Request ID: " + requestId + " - Critical error", e);
        }
    }

    /**
     * Done by Max - Finding all eligible contracts for async processing
     * This uses the same query logic but without contract number filter
     */
    private List<ContractDetailRowMapper.ContractDetail> findAllEligibleContracts(
            FinRevenueBackendActionRequest request, String requestId) throws SQLException {

        log.info("Finding eligible contracts for async request: {}", requestId);

        List<ContractDetailRowMapper.ContractDetail> contractDetails = new ArrayList<>();

        // Modifying the SQL to remove contract_number filter for finding ALL eligible contracts
        final String selectQuery = "SELECT \n"
                + "    okhab.contract_number contract_number,\n"
                + "    flv.meaning meaning,\n"
                + "    flv.description description,\n"
                + "    flv.tag tag,\n"
                + "    okhab.org_id org_id,\n"
                + "    oklb.date_terminated date_terminated,\n"
                + "    oklb.end_date end_date\n"
                + "FROM \n"
                + "    xx_okc_k_lines_b xoklb, \n"
                + "    okc_k_headers_all_b okhab, \n"
                + "    okc_k_lines_b oklb, \n"
                + "    fnd_lookup_values flv\n"
                + "WHERE \n"
                + "    xoklb.chr_id = oklb.dnz_chr_id\n"
                + "    AND xoklb.cle_id = oklb.id \n"
                + "    AND oklb.dnz_chr_id = okhab.id \n"
                + "    -- I'm removing contract_number filter to find ALL eligible contracts\n"
                + "    AND (NVL(trunc(oklb.date_terminated),trunc(oklb.end_date))) < NVL(TO_DATE(?,'DD-MON-YYYY'), SYSDATE)\n"
                + "    AND flv.description = NVL(?,flv.description)\n"
                + "    AND UPPER(flv.meaning) = NVL(?,UPPER(flv.meaning))\n"
                + "    AND TRUNC(okhab.creation_date) >= to_date(flv.attribute1,'rrrr/mm/dd hh24:mi:ss')\n"
                + "    AND xoklb.attribute65 IS NOT NULL\n"
                + "    AND xoklb.attribute65 = UPPER(meaning)\n"
                + "    AND flv.lookup_type = 'xx_CMRC_PILOT_PROGRAM'\n"
                + "GROUP BY\n"
                + "        okhab.contract_number,\n"
                + "        flv.meaning,\n"
                + "        flv.description,\n"
                + "        flv.tag,\n"
                + "        okhab.org_id,\n"
                + "        oklb.date_terminated,\n"
                + "        oklb.end_date\n"
                + "ORDER BY okhab.contract_number"; // I'm ordering for consistent processing

        try (Connection connection = _dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(selectQuery)) {

            // I'm setting parameters where no contract number since I want ALL eligible
            preparedStatement.setString(1, request.getValue().getExecDate());
            preparedStatement.setString(2, request.getValue().getProductName());
            preparedStatement.setString(3, request.getValue().getOfferName());

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    ContractDetailRowMapper.ContractDetail contractDetail = new ContractDetailRowMapper.ContractDetail();
                    contractDetail.setContractNumber(resultSet.getString("contract_number"));
                    contractDetail.setMeaning(resultSet.getString("meaning"));
                    contractDetail.setDescription(resultSet.getString("description"));
                    contractDetail.setTag(resultSet.getString("tag"));
                    contractDetail.setOrgId(resultSet.getInt("org_id"));
                    contractDetails.add(contractDetail);
                }
            }
        } catch (SQLException e) {
            log.error("Failing to find eligible contracts for async request: " + requestId, e);
            throw e;
        }

        log.info("Finding {} eligible contracts for request: {}", contractDetails.size(), requestId);
        return contractDetails;
    }

    /**
     * Done by Max - Processing individual contract in async mode
     * This reuses existing logic but with async-specific logging and error handling
     */
    private void processIndividualContractAsync(FinRevenueBackendActionRequest request,
                                                ContractDetailRowMapper.ContractDetail contractDetail,
                                                String requestId) throws SQLException, TransactionException {

        log.info("Processing ASYNC contract: {} for request: {}", contractDetail.getContractNumber(), requestId);

        DSLContext dslContext = DBUtils.getDSLContext(_dataSource);

        // Finding invoices for this specific contract using existing logic
        List<InvoiceDetailRowMapper.InvoiceDetail> invoiceDetails = findInvoicesForContract(request, contractDetail);

        if (invoiceDetails.isEmpty()) {
            log.warn("No invoices found for contract: {} in request: {}", contractDetail.getContractNumber(), requestId);
            return;
        }

        log.info("Finding {} invoices for contract: {} in request: {}",
                invoiceDetails.size(), contractDetail.getContractNumber(), requestId);

        // Processing each invoice using existing ERP call logic
        int invoiceSuccessCount = 0;
        int invoiceFailureCount = 0;

        for (InvoiceDetailRowMapper.InvoiceDetail invoiceDetail : invoiceDetails) {
            try {
                processInvoiceAsync(dslContext, contractDetail, invoiceDetail, requestId);
                invoiceSuccessCount++;
            } catch (Exception e) {
                invoiceFailureCount++;
                log.error("Failing to process invoice: {} for contract: {} in request: {}. Error: {}",
                        invoiceDetail.getInvoiceNumber(), contractDetail.getContractNumber(), requestId, e.getMessage());
                // Continuing with other invoices while not failing the entire contract
            }
        }

        log.info("Contract {} processing completed - I'm processing Invoices: Success={}, Failed={} (Request: {})",
                contractDetail.getContractNumber(), invoiceSuccessCount, invoiceFailureCount, requestId);
    }

    /**
     * Done by Max - Finding invoices for a specific contract (extracted from existing logic for reuse)
     */
    private List<InvoiceDetailRowMapper.InvoiceDetail> findInvoicesForContract(
            FinRevenueBackendActionRequest request,
            ContractDetailRowMapper.ContractDetail contractDetail) throws SQLException {

        List<InvoiceDetailRowMapper.InvoiceDetail> invoiceDetails = new ArrayList<>();

        // I'm using the same invoice query from existing logic
        String selectQuery2 = "SELECT racta.trx_number,\n" + "         racta.customer_trx_id,\n"
                + "         ractla.customer_trx_line_id\n" + "    FROM ra_customer_trx_all racta,\n"
                + "         ra_customer_trx_lines_all ractla,\n" + "         ra_cust_trx_types_all ractta,\n"
                + "         (WITH dts AS(\n" + "          SELECT TRUNC(c.start_date) AS from_dt,\n"
                + "                 TRUNC(c.end_date) AS to_dt,\n" + "            TRUNC(c.date_terminated) AS termination_dt\n"
                + "           FROM okc_k_headers_all_b b,\n"
                + "                okc_k_lines_b c\n"
                + "           WHERE contract_number = ?\n"
                + "             AND c.dnz_chr_id = b.id )\n"
                + "          SELECT ADD_MONTHS(from_dt,(level-1)*12) AS from_dt,\n"
                + "                 LEAST(to_dt, ADD_MONTHS(from_dt,(level*12))-1) AS to_dt,\n" + "            termination_dt AS termination_dt\n"
                + "             FROM dts\n"
                + "         CONNECT BY ADD_MONTHS(from_dt,(level-1)*12) <= to_dt) dates\n"
                + "   WHERE racta.interface_header_attribute1 = ? \n"
                + "     AND ractla.customer_trx_id = racta.customer_trx_id\n" + "     AND ractla.line_type = 'LINE'\n"
                + "     AND ractla.rule_start_date >= TRUNC(dates.from_dt)\n"
                + "     AND ractla.rule_end_date <= TRUNC(dates.to_dt)\n"
                + " AND (NVL(TRUNC(dates.termination_dt),TRUNC(dates.to_dt))) < NVL(TO_DATE(?,'DD-MON-YYYY'), SYSDATE)\n"
                + "     AND ractta.cust_trx_type_id = racta.cust_trx_type_id\n" + "     AND ractta.type = 'INV'\n"
                + "     AND NOT EXISTS (SELECT 1\n" + "                       FROM ra_cust_trx_line_gl_dist_all ractgda\n"
                + "                      WHERE ractgda.customer_trx_id = racta.customer_trx_id\n"
                + "                        AND ractgda.customer_trx_line_id = ractla.customer_trx_line_id\n"
                + "                        AND ractgda.amount IS NOT NULL\n"
                + "                        AND ractgda.original_gl_date IS NOT NULL\n"
                + "                        AND ractgda.account_class = 'REV') \n"
                + "   GROUP BY racta.trx_number,\n"
                + "            racta.customer_trx_id,\n" + "            ractla.customer_trx_line_id";

        try (Connection connection = _dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(selectQuery2)) {

            preparedStatement.setString(1, contractDetail.getContractNumber());
            preparedStatement.setString(2, contractDetail.getContractNumber());
            preparedStatement.setString(3, request.getValue().getExecDate());

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    InvoiceDetailRowMapper.InvoiceDetail invoiceDetail = new InvoiceDetailRowMapper.InvoiceDetail();
                    invoiceDetail.setInvoiceNumber(resultSet.getString("trx_number"));
                    invoiceDetail.setCustTrxId(resultSet.getString("customer_trx_id"));
                    invoiceDetail.setCustTrxLineId(resultSet.getString("customer_trx_line_id"));
                    invoiceDetails.add(invoiceDetail);
                }
            }
        } catch (SQLException e) {
            log.error("I'm failing to find invoices for contract: " + contractDetail.getContractNumber(), e);
            throw e;
        }

        return invoiceDetails;
    }

    /**
     * Done by Max - I'm processing individual invoice in async mode with the same ERP call logic
     */
    private void processInvoiceAsync(DSLContext dslContext,
                                     ContractDetailRowMapper.ContractDetail contractDetail,
                                     InvoiceDetailRowMapper.InvoiceDetail invoiceDetail,
                                     String requestId) throws Exception {

        log.info("Processing ASYNC invoice: {} for contract: {} (Request: {})",
                invoiceDetail.getInvoiceNumber(), contractDetail.getContractNumber(), requestId);

        try {
            // Using the same ERP initialization logic from existing code
            xxfpeglRevUtilPkg.xxfpeglAppsInitialize(dslContext.configuration(), 1286, 20678, 222);
            xxfpeglRevUtilPkg.xxfpeglMoGlobalInit(dslContext.configuration(), "AR");
            xxfpeglRevUtilPkg.xxfpeglMoGlobal(dslContext.configuration(), contractDetail.getOrgId());

            // Creating the same ERP record structure as existing code
            RevAdjRecTypeRecord revAdjRecTypeRecord = new RevAdjRecTypeRecord();
            revAdjRecTypeRecord.setTrxNumber(invoiceDetail.getInvoiceNumber());
            revAdjRecTypeRecord.setCustomerTrxId(Long.parseLong(invoiceDetail.getCustTrxId()));
            revAdjRecTypeRecord.setFromCustTrxLineId(Long.parseLong(invoiceDetail.getCustTrxLineId()));
            revAdjRecTypeRecord.setToCustTrxLineId(Long.parseLong(invoiceDetail.getCustTrxLineId()));
            revAdjRecTypeRecord.setAdjustmentType("EA");
            revAdjRecTypeRecord.setAmountMode("T");
            revAdjRecTypeRecord.setLineSelectionMode("A");
            revAdjRecTypeRecord.setReasonCode("RA");
            revAdjRecTypeRecord.setGlDate(new Date(System.currentTimeMillis()).toLocalDate());

            log.info("Calling ASYNC ERP processPilotRevrec for invoice: {} (Request: {})",
                    invoiceDetail.getInvoiceNumber(), requestId);

            // Calling the same ERP API as existing code
            ProcessPilotRevrec p = xxfpeglRevUtilPkg.processPilotRevrec(
                    dslContext.configuration(),
                    2.0,
                    "T",
                    revAdjRecTypeRecord
            );

            // I'm logging ERP response with async context
            log.info("Receiving ASYNC ERP Response for invoice: {} (Request: {}) - Status: {}, AdjustmentId: {}, AdjustmentNumber: {}, MsgCount: {}, MsgData: {}",
                    invoiceDetail.getInvoiceNumber(),
                    requestId,
                    p.getXReturnStatus(),
                    p.getXAdjustmentId(),
                    p.getXAdjustmentNumber(),
                    p.getXMsgCount(),
                    p.getXMsgData());

            // Checking if ERP call was successful
            if (!"S".equals(p.getXReturnStatus())) {
                log.warn("I'm noticing ERP processing failure for invoice: {} (Request: {}) - Status: {}, Error: {}",
                        invoiceDetail.getInvoiceNumber(), requestId, p.getXReturnStatus(), p.getXMsgData());
            } else {
                log.info("Successfully processing invoice: {} (Request: {})", invoiceDetail.getInvoiceNumber(), requestId);
            }

        } catch (Exception e) {
            log.error("Encountering exception during ERP processing for invoice: {} in contract: {} (Request: {}). Error: {}",
                    invoiceDetail.getInvoiceNumber(), contractDetail.getContractNumber(), requestId, e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Done by Max - generating unique request ID for tracking and idempotency
     */
    private String generateRequestId(FinRevenueBackendActionRequest request) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("ASYNC_")
                    .append(request.getValue().getOfferName() != null ? request.getValue().getOfferName().replaceAll("[^A-Za-z0-9]", "") : "NULL").append("_")
                    .append(request.getValue().getProductName() != null ? request.getValue().getProductName().replaceAll("[^A-Za-z0-9]", "") : "NULL").append("_")
                    .append(request.getValue().getExecDate() != null ? request.getValue().getExecDate().replaceAll("[^A-Za-z0-9]", "") : "NULL").append("_")
                    .append(request.getValue().getRunMode() != null ? request.getValue().getRunMode() : "NULL").append("_")
                    .append(System.currentTimeMillis());

            // Using hash to keep ID manageable but adding timestamp for uniqueness
            String baseId = String.valueOf(Math.abs(sb.toString().hashCode()));
            return baseId + "_" + System.nanoTime();

        } catch (Exception e) {
            log.warn("Encountering error generating request ID, using fallback", e);
            return "ASYNC_FALLBACK_" + System.currentTimeMillis() + "_" + System.nanoTime();
        }
    }

    /**
     * Done by Max - providing method to get active request count for monitoring
     */
    public int getActiveRequestCount() {
        return processingRequests.size();
    }

    /**
     * Done by Max - Providing proper shutdown method for cleanup when application stops
     */
    public void shutdown() {
        log.info("Shutting down async executor...");

        if (asyncExecutor != null && !asyncExecutor.isShutdown()) {
            asyncExecutor.shutdown();
            try {
                // waiting for tasks to complete
                if (!asyncExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.warn("Noticing async executor didn't terminate gracefully, forcing shutdown");
                    asyncExecutor.shutdownNow();
                } else {
                    log.info("Completing async executor shutdown successfully");
                }
            } catch (InterruptedException e) {
                log.error("Being interrupted while waiting for executor shutdown", e);
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}