using System.Linq;

namespace MathNet.Numerics.LinearAlgebra.Extension
{
    public static class MatrixExtensionMethods
    {
        public static Matrix<float> Reshape(this Matrix<float> m, int rowSize, int columnSize)
        {
            return Matrix<float>.Build.Dense(rowSize, columnSize, m.Enumerate().ToArray());
        }
    }
}
