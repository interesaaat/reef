using System.Linq;

namespace MathNet.Numerics.LinearAlgebra.Extension
{
    public static class MatrixExtensionMethods
    {
        public static Matrix<float> Reshape(this Matrix<float> m, int size)
        {
            return Matrix<float>.Build.Dense(m.RowCount * size, m.ColumnCount / size, m.Enumerate().ToArray());
        }
    }
}
