-- 1. Standardizing Sale Date
-- Adding a new clean date column
ALTER TABLE dbo.NashvilleHousing
ADD SaleDate_Clean DATE

UPDATE dbo.NashvilleHousing
SET SaleDate_Clean = CAST(SaleDate AS DATE)

SELECT SaleDate, SaleDate_Clean
FROM dbo.NashvilleHousing


-- 2. Filling Missing PropertyAddress
-- If two rows share the same ParcelID, they should share the same address. (self-join)
-- a = row missing address, b = row with address
UPDATE a
SET a.PropertyAddress = b.PropertyAddress
FROM dbo.NashvilleHousing a JOIN dbo.NashvilleHousing b ON a.ParcelID = b.ParcelID AND a.[UniqueID ] <> b.[UniqueID ]
WHERE a.PropertyAddress IS NULL


-- 3. Splitting Property Address
-- Everything before the comma = Address, Everything after the comma = City
ALTER TABLE dbo.NashvilleHousing
ADD PropertySplitAddress NVARCHAR(255), PropertySplitCity NVARCHAR(255)

UPDATE dbo.NashvilleHousing
SET PropertySplitAddress = LEFT(PropertyAddress, CHARINDEX(',', PropertyAddress) - 1), PropertySplitCity = RIGHT(PropertyAddress, LEN(PropertyAddress) - CHARINDEX(',', PropertyAddress))


-- 4. Split Owner Address
-- Replace commas with dots so SQL can count pieces. (PARSENAME splits from right to left)
ALTER TABLE dbo.NashvilleHousing
ADD OwnerSplitAddress NVARCHAR(255), OwnerSplitCity NVARCHAR(255), OwnerSplitState NVARCHAR(50)

UPDATE dbo.NashvilleHousing
SET OwnerSplitAddress = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3), OwnerSplitCity = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2), OwnerSplitState = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1)


-- 5. Normalizing SoldAsVacant
-- Checking Data type (needs to be text)
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'NashvilleHousing' AND COLUMN_NAME = 'SoldAsVacant'

-- Creating a text column from the bit
ALTER TABLE dbo.NashvilleHousing
ADD SoldAsVacant_Text VARCHAR(3)

UPDATE dbo.NashvilleHousing
SET SoldAsVacant_Text =
    CASE 
        WHEN SoldAsVacant = 1 THEN 'Yes'
        WHEN SoldAsVacant = 0 THEN 'No'
        ELSE NULL
    END;

SELECT SoldAsVacant, SoldAsVacant_Text
FROM dbo.NashvilleHousing

-- 6. Removing duplicates
-- Keep the first row, delete the rest. (rn = 1 ? keep, rn > 1 ? delete)
WITH Duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ParcelID, PropertyAddress, SalePrice, SaleDate, LegalReference
               ORDER BY [UniqueID ]
           ) AS rn
    FROM dbo.NashvilleHousing
)
DELETE
FROM Duplicates
WHERE rn > 1


-- 7. Dropping split and cleaned columns
ALTER TABLE dbo.NashvilleHousing
DROP COLUMN OwnerAddress, TaxDistrict, PropertyAddress, SaleDate, SoldAsVacant


-- 8. Final dataset preview
SELECT TOP 10 *
FROM dbo.NashvilleHousing

-- Checking duplicates
SELECT COUNT(*) AS TotalRows,
       COUNT(DISTINCT ParcelID) AS UniqueParcels
FROM dbo.NashvilleHousing




