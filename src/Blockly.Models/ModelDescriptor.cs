using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Blockly.Models
{
    public class ModelDescriptor
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string LibraryBlobId { get; set; }

        public string DefinitionsBlobId { get; set; }

        public string ToolBoxBlobId { get; set; }
    }
}
