use alloy_json_abi::{Event, Function};
use alloy_primitives::hex;
use anyhow::{Result, bail};

use super::{ManifestAbiCall, ManifestAbiEvent};

impl ManifestAbiEvent {
    pub fn parsed_event_view(&self) -> Result<ParsedManifestAbiEvent> {
        let fragment = self.fragment.trim();
        if !fragment.starts_with("event ") {
            bail!("ABI event {} must use an event fragment", self.name);
        }

        let event = Event::parse(fragment)?;
        if event.name != self.name {
            bail!("ABI event {} has fragment name {}", self.name, event.name);
        }

        Ok(ParsedManifestAbiEvent(event))
    }

    pub fn parsed_event(&self) -> Result<Event> {
        Ok(self.parsed_event_view()?.into_event())
    }

    pub fn canonical_signature(&self) -> Result<String> {
        Ok(self.parsed_event_view()?.canonical_signature())
    }

    pub fn topic0(&self) -> Result<Option<String>> {
        Ok(self.parsed_event_view()?.topic0())
    }
}

#[derive(Clone, Debug)]
pub struct ParsedManifestAbiEvent(Event);

impl ParsedManifestAbiEvent {
    fn into_event(self) -> Event {
        self.0
    }

    pub fn canonical_signature(&self) -> String {
        self.0.signature()
    }

    pub fn topic0(&self) -> Option<String> {
        (!self.0.anonymous).then(|| prefixed_hex(self.0.selector().as_slice()))
    }
}

impl ManifestAbiCall {
    pub fn parsed_function_view(&self) -> Result<ParsedManifestAbiFunction> {
        let fragment = self.fragment.trim();
        if !fragment.starts_with("function ") {
            bail!("ABI call {} must use a function fragment", self.name);
        }

        let function = Function::parse(fragment)?;
        if function.name != self.name {
            bail!("ABI call {} has fragment name {}", self.name, function.name);
        }

        Ok(ParsedManifestAbiFunction(function))
    }

    pub fn parsed_function(&self) -> Result<Function> {
        Ok(self.parsed_function_view()?.into_function())
    }

    pub fn canonical_signature(&self) -> Result<String> {
        Ok(self.parsed_function_view()?.canonical_signature())
    }

    pub fn selector(&self) -> Result<String> {
        Ok(self.parsed_function_view()?.selector())
    }
}

#[derive(Clone, Debug)]
pub struct ParsedManifestAbiFunction(Function);

impl ParsedManifestAbiFunction {
    fn into_function(self) -> Function {
        self.0
    }

    pub fn canonical_signature(&self) -> String {
        self.0.signature()
    }

    pub fn selector(&self) -> String {
        prefixed_hex(self.0.selector().as_slice())
    }
}

fn prefixed_hex(bytes: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(bytes))
}
