package com.banking.paymentservice.service;

import com.banking.paymentservice.dto.CreatePaymentRequest;
import com.banking.paymentservice.dto.PaymentOrderResponse;
import com.banking.paymentservice.entity.Payment;
import com.banking.paymentservice.entity.PaymentStatus;
import com.banking.paymentservice.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${razorpay.key-id}")
    private String keyId;

    @Value("${razorpay.key-secret}")
    private String keySecret;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";


    /**
     * method to create the payment request
     * FLOW:
     * 1. Create order in razorpay
     * 2. Save payment record in DB
     * 3. Return order details to UI
     * 4. frontend shows the razorpay checkout page
     * 5. User pays
     * 6. razorpay calls webhook(status of the payment for the backend)
     * @param request
     * @return
     */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {

        log.info("Creating payment order for account: {} amount: {}", request.getAccountNumber(), request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

        // amount conversion to lower version
        int convertedAmount = request.getAmount()
                .multiply(BigDecimal.valueOf(100))
                .intValue();

        JSONObject orderObject = new JSONObject();
        orderObject.put("amount", convertedAmount);
        orderObject.put("currency", "USD");
        orderObject.put("receipt", "rcpt_" + System.currentTimeMillis() +
                UUID.randomUUID().toString().replace("-", "").substring(0, 10)
        );

        Order razorpayOrder = razorpayClient.orders.create(orderObject);

        log.info("Razorpay order is created: {}", razorpayOrder.get("id").toString());

        // persist order details to db
        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAmount(request.getAmount());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setStatus(PaymentStatus.CREATED);
        payment.setCurrency("USD");
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);
        return mapToResponse(savedPayment);
    }

    /**
     *
     * @param payload
     */
    public void handleWebHook(Map<String, Object> payload) {

        log.info("Received razorpay webhook: {}", payload.get("event"));

        String event = (String) payload.get("event");

        if("payment.captured".equals(event)) {
            handlePaymentSuccess(payload);
        } else if("payment.failed".equals(event)) {
            handlePaymentFailure(payload);
        }

    }

    /**
     * method to handle the payment success
     * and push the event to kafka
     * @param payload
     */
    private void handlePaymentSuccess(Map<String, Object> payload) {

        try {
            Map<String, Object> paymentData = extractPaymentData(payload);

            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found with order id: " + orderId));

            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            Map<String, Object> paymentSuccessEvent = new HashMap<>();
            paymentSuccessEvent.put("paymentId", payment.getId());
            paymentSuccessEvent.put("accountNumber", payment.getAccountNumber());
            paymentSuccessEvent.put("amount", payment.getAmount());
            paymentSuccessEvent.put("razorpayPaymentId", paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), paymentSuccessEvent);

            log.info("Payment success for payment id: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error occurred while handling payment success {}", e.getMessage());
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {

        try {
            Map<String, Object> paymentData = extractPaymentData(payload);

            String orderId = (String) paymentData.get("order_id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(() -> new RuntimeException("Payment not found with order id: " + orderId));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via razorpay client");
            paymentRepository.save(payment);

            Map<String, Object> paymentFailureEvent = new HashMap<>();
            paymentFailureEvent.put("paymentId", payment.getId());
            paymentFailureEvent.put("accountNumber", payment.getAccountNumber());
            paymentFailureEvent.put("amount", payment.getAmount());
            paymentFailureEvent.put("reason", payment.getFailureReason());

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), paymentFailureEvent);

            log.warn("Payment failure for payment id: {}", payment.getId());

        } catch (Exception e) {
            log.error("Error occurred while handling payment failure {}", e.getMessage());
        }

    }

    private Map<String, Object> extractPaymentData(Map<String, Object> payload) {

        Map<String, Object> payloadData = (Map<String, Object>) payload.get("payload");

        Map<String, Object> paymentWrapper = (Map<String, Object>) payloadData.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");
    }

    private PaymentOrderResponse mapToResponse(Payment payment) {
        return PaymentOrderResponse.builder()
                .paymentId(payment.getId())
                .razorpayOrderId(payment.getRazorpayOrderId())
                .razorpayKeyId(keyId)
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .status(payment.getStatus().toString())
                .build();
    }

}
