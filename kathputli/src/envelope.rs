use tokio::sync::oneshot;

/// Request-response envelope — wraps a command with its reply channel.
///
/// Use this when you want to define message enums where every variant carries
/// a reply sender without writing the oneshot boilerplate each time.
///
/// ```rust,ignore
/// type CreatePatient = Envelope<CreatePatientRequest, Result<Patient>>;
///
/// enum PatientMsg {
///     Create(Envelope<CreatePatientRequest, Result<Patient>>),
///     // ...
/// }
/// ```
pub struct Envelope<Req, Resp> {
    pub req: Req,
    pub reply: oneshot::Sender<Resp>,
}

impl<Req, Resp> Envelope<Req, Resp> {
    pub fn new(req: Req, reply: oneshot::Sender<Resp>) -> Self {
        Self { req, reply }
    }
}
