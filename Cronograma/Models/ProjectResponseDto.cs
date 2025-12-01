using System.Collections.Generic;

namespace Cronograma.Models
{
    public class ProjectResponseDto
    {
        public string ProjectName { get; set; } = default!;
        public List<ColumnDefinitionDto> Columns { get; set; } = new();
        public List<Dictionary<string, object?>> Rows { get; set; } = new();
    }
}
