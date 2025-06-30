-- =============================================
-- Author:		AI Generated SQL Query
-- Create date: 30/06/2025
-- Description:	In this example, the "ProductReview" table is created with a foreign key relationship to the "Product" table using the "ProductID" field. The "ReviewInfo" field in the "ProductReview" table stores JSON data related to product reviews.
-- =============================================

-- Create the ProductReview table
CREATE TABLE ProductReview (
    ReviewID INT PRIMARY KEY,
    ProductID INT,
    ReviewInfo NVARCHAR(MAX),
    CONSTRAINT FK_ProductReview_ProductID FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
);

-- Insert a sample record with JSON data in the ReviewInfo field
INSERT INTO ProductReview (ReviewID, ProductID, ReviewInfo)
VALUES (1, 680, '{"Rating": 5, "Comment": "Great product, highly recommended"}');

-- Query to retrieve Product information along with related Product Review
SELECT p.ProductID, p.Name, pr.ReviewInfo
FROM Product p
LEFT JOIN ProductReview pr ON p.ProductID = pr.ProductID;