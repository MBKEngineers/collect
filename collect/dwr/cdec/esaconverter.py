def tabint1d(x, values_table, x_column, y_column, precision=None):
    """
    Linear interpolation between points in a table.
    Assumes table is sorted ascending by x_column.

    Arguments:
        x (float): value for lookup
        values_table (list): a list of lists representing interpolation table
        x_column (int): column to use for lookup dimension
        y_column (int): column to use for result dimension
        precision (int): optional parameter for setting the number of decimal points to round to
    Returns:
        result (float): interpolated value
    """
    # Extract x and y values from table
    x_values = [row[x_column] for row in values_table]
    y_values = [row[y_column] for row in values_table]
    
    # Check if x is within bounds
    if x < min(x_values) or x > max(x_values):
        raise ValueError(f"Value {x} is outside table bounds [{min(x_values)}, {max(x_values)}]")

    # Find surrounding points for interpolation
    for i in range(len(x_values)-1):
        if x_values[i] <= x <= x_values[i+1]:
            x0, x1 = x_values[i], x_values[i+1]
            y0, y1 = y_values[i], y_values[i+1]
            
            # Linear interpolation
            result = y0 + (x - x0) * (y1 - y0) / (x1 - x0)
            
            # Round if precision specified
            if precision is not None:
                result = round(result, precision)
                
            return float(result)

    return float(y_values[-1])  # Return last value if x equals max x_value

class ElevationStorageConverter:
    """Converts between elevation and storage using an ESA table"""
    
    def __init__(self, esa_file):
        """
        Initialize with path to ESA file.
        Expected CSV format: elevation,storage,area
        No header row.
        
        Arguments:
            esa_file (str): Path to ESA CSV file
        """
        self.data = []
        self.read_esa_file(esa_file)

    def read_esa_file(self, filepath):
        """
        Read ESA data from CSV file
        
        Arguments:
            filepath (str): Path to ESA CSV file
        """
        with open(filepath, 'r') as f:
            for line in f:
                # Skip empty lines
                if not line.strip():
                    continue
                # Convert line to list of floats
                try:
                    values = [float(x) for x in line.strip().split(',')]
                    if len(values) >= 2:  # Need at least elevation and storage
                        self.data.append(values)
                except ValueError:
                    continue  # Skip header rows or invalid lines
        
        # Sort by elevation
        self.data.sort(key=lambda x: x[0])

    def storage_to_elevation(self, storage):
        """
        Convert storage value to elevation
        
        Arguments:
            storage (float): Storage value in acre-feet
        Returns:
            float: Elevation value in feet
        """
        return tabint1d(storage, self.data, 1, 0)

    def elevation_to_storage(self, elevation):
        """
        Convert elevation value to storage
        
        Arguments:
            elevation (float): Elevation value in feet
        Returns:
            float: Storage value in acre-feet
        """
        return tabint1d(elevation, self.data, 0, 1)

if __name__ == "__main__":
    # Example usage:
    converter = ElevationStorageConverter("/Users/sharp/Desktop/hydrology_home/resops/resops/folsom/ratings/elev_stor_area.csv")
    elevation = converter.storage_to_elevation(100000)
    print(elevation) 
