
package com.bajajfinserv.webhook;

import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class WebhookStartupRunner implements CommandLineRunner {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    // Your real details
    private static final String NAME = "Sai Sandeep Koneti";
    private static final String REG_NO = "22BCE20185";
    private static final String EMAIL = "saisandeepkoneti43p@gmail.com";

    public WebhookStartupRunner() {
        this.restTemplate = new RestTemplate();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Starting webhook process...");

        // Step 1: Generate webhook
        String generateUrl = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

        String requestBody = """
            {
              "name": "%s",
              "regNo": "%s",
              "email": "%s"
            }
            """.formatted(NAME, REG_NO, EMAIL);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(requestBody, headers);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                generateUrl,
                HttpMethod.POST,
                request,
                String.class
            );

            System.out.println("Webhook generated successfully!");
            String responseBody = response.getBody();
            System.out.println("Response: " + responseBody);

            // Parse response to get webhook URL and accessToken
            JsonNode jsonNode = objectMapper.readTree(responseBody);
            String webhookUrl = jsonNode.get("webhook").asText();
            String accessToken = jsonNode.get("accessToken").asText();

            System.out.println("Webhook URL: " + webhookUrl);
            System.out.println("Access Token received");

            // Step 2: Determine which question based on regNo
            int lastTwoDigits = Integer.parseInt(REG_NO.substring(REG_NO.length() - 2));
            boolean isOdd = lastTwoDigits % 2 != 0;

            System.out.println("Registration number last two digits: " + lastTwoDigits);
            System.out.println("Question type: " + (isOdd ? "Question 1 (Odd)" : "Question 2 (Even)"));

            // Step 3: Solve the SQL problem
            String finalQuery = getSqlQuery(isOdd);

            System.out.println("Final SQL Query: " + finalQuery);

            // Step 4: Submit the solution
            submitSolution(webhookUrl, accessToken, finalQuery);

        } catch (Exception e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Returns the final SQL query as a single-line string.
     * For you (22BCE20185 -> 85 -> odd), Question 1 is used.
     */
    private String getSqlQuery(boolean isOdd) {
        if (isOdd) {
            // QUESTION 1:
            // Highest salaried employee per department (sum of payments),
            // excluding payments made on the 1st day of the month,
            // plus EMPLOYEE_NAME and AGE.
            String sql = """
                WITH filtered_payments AS (
                    SELECT
                        p.EMP_ID,
                        SUM(p.AMOUNT) AS total_salary
                    FROM PAYMENTS p
                    WHERE EXTRACT(DAY FROM p.PAYMENT_TIME) <> 1
                    GROUP BY p.EMP_ID
                ),
                emp_totals AS (
                    SELECT
                        e.EMP_ID,
                        e.FIRST_NAME,
                        e.LAST_NAME,
                        e.DOB,
                        e.DEPARTMENT,
                        fp.total_salary
                    FROM EMPLOYEE e
                    JOIN filtered_payments fp
                        ON e.EMP_ID = fp.EMP_ID
                ),
                ranked AS (
                    SELECT
                        d.DEPARTMENT_NAME,
                        et.total_salary AS SALARY,
                        CONCAT(et.FIRST_NAME, ' ', et.LAST_NAME) AS EMPLOYEE_NAME,
                        FLOOR(DATEDIFF(CURDATE(), et.DOB) / 365.25) AS AGE,
                        ROW_NUMBER() OVER (
                            PARTITION BY et.DEPARTMENT
                            ORDER BY et.total_salary DESC
                        ) AS rn
                    FROM emp_totals et
                    JOIN DEPARTMENT d
                        ON d.DEPARTMENT_ID = et.DEPARTMENT
                )
                SELECT
                    DEPARTMENT_NAME,
                    SALARY,
                    EMPLOYEE_NAME,
                    AGE
                FROM ranked
                WHERE rn = 1
                ORDER BY DEPARTMENT_NAME
                """;

            // Convert to single line to avoid JSON issues
            return sql.trim().replaceAll("\\s+", " ");
        } else {
            // QUESTION 2 (not used for your reg no, but kept for completeness)
            String sql = """
                WITH emp_totals AS (
                    SELECT
                        e.EMP_ID,
                        e.FIRST_NAME,
                        e.LAST_NAME,
                        e.DOB,
                        e.DEPARTMENT,
                        SUM(p.AMOUNT) AS total_salary
                    FROM EMPLOYEE e
                    JOIN PAYMENTS p
                        ON e.EMP_ID = p.EMP_ID
                    GROUP BY
                        e.EMP_ID,
                        e.FIRST_NAME,
                        e.LAST_NAME,
                        e.DOB,
                        e.DEPARTMENT
                )
                SELECT
                    d.DEPARTMENT_NAME,
                    AVG(FLOOR(DATEDIFF(CURDATE(), et.DOB) / 365.25)) AS AVERAGE_AGE,
                    GROUP_CONCAT(CONCAT(et.FIRST_NAME, ' ', et.LAST_NAME) ORDER BY et.FIRST_NAME SEPARATOR ', ') AS EMPLOYEE_LIST
                FROM emp_totals et
                JOIN DEPARTMENT d
                    ON d.DEPARTMENT_ID = et.DEPARTMENT
                WHERE et.total_salary > 70000
                GROUP BY
                    d.DEPARTMENT_ID,
                    d.DEPARTMENT_NAME
                ORDER BY d.DEPARTMENT_ID DESC
                """;

            return sql.trim().replaceAll("\\s+", " ");
        }
    }

    private void submitSolution(String webhookUrl, String accessToken, String finalQuery) throws Exception {
        System.out.println("\nSubmitting solution to webhook...");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // IMPORTANT: According to the problem statement:
        // Authorization: <accessToken>  (NO "Bearer " prefix)
        headers.set("Authorization", accessToken);

        FinalQueryRequest body = new FinalQueryRequest(finalQuery);
        HttpEntity<FinalQueryRequest> request = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                webhookUrl,          // this is "https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/JAVA"
                HttpMethod.POST,
                request,
                String.class
            );

            System.out.println("Solution submitted successfully!");
            System.out.println("Response status: " + response.getStatusCode());
            System.out.println("Response body: " + response.getBody());

        } catch (Exception e) {
            System.err.println("Error submitting solution: " + e.getMessage());
            throw e;
        }
    }
}
